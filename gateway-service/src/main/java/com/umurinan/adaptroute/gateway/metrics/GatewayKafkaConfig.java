package com.umurinan.adaptroute.gateway.metrics;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer, consumer, and admin client configuration for the gateway service.
 *
 * <p>Producer settings are tuned for the research workloads:</p>
 * <ul>
 *   <li>{@code linger.ms=5} — batches messages for up to 5 ms to model
 *       production-realistic throughput; adds up to 5 ms additional latency
 *       to individual Kafka sends.</li>
 *   <li>{@code batch.size=32768} — 32 KB batch size; balances latency and throughput.</li>
 *   <li>{@code compression.type=lz4} — reduces network I/O for large payloads.</li>
 *   <li>{@code acks=all} — required for transactional exactly-once delivery.</li>
 *   <li>{@code transactionIdPrefix=adaptroute-tx-} — enables Kafka producer
 *       transactions for EXACTLY_ONCE delivery guarantee.</li>
 * </ul>
 *
 * <p>Consumer settings use {@code isolation.level=read_committed} so that
 * transactionally-produced messages are only visible after commit, and
 * {@code MANUAL_IMMEDIATE} acknowledgement for precise latency measurement.</p>
 *
 * <p>Consumer concurrency is set to 4 threads, matching the 4 Kafka partitions
 * in the testbed, so each thread owns exactly one partition.</p>
 */
@EnableKafka
@Configuration
public class GatewayKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:gateway-consumer-group}")
    private String consumerGroupId;

    // =========================================================================
    // Producer
    // =========================================================================

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(ProducerConfig.RETRIES_CONFIG,                3);
        props.put(ProducerConfig.LINGER_MS_CONFIG,              5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,             32_768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,       "lz4");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,     true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        DefaultKafkaProducerFactory<String, String> factory =
                new DefaultKafkaProducerFactory<>(props);
        // Transactional ID prefix enables producer transactions for EXACTLY_ONCE semantics
        factory.setTransactionIdPrefix("adaptroute-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Transaction manager for Kafka producer transactions.
     * Used by {@code kafkaTemplate.executeInTransaction()} in the dispatcher.
     */
    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager(
            ProducerFactory<String, String> pf) {
        return new KafkaTransactionManager<>(pf);
    }

    // =========================================================================
    // Consumer
    // =========================================================================

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                  consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         500);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,          1);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        500);
        // Read only committed messages to honour the producer transaction boundary
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(4);   // one thread per partition (4 partitions in testbed)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    // =========================================================================
    // Admin
    // =========================================================================

    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ));
    }

    @Bean
    public AdminClient kafkaAdminClient() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,     "5000",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000"
        ));
    }
}
