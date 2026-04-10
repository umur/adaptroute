package com.umurinan.adaptroute.gateway.consumer;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes messages from all adaptive Kafka topics.
 *
 * <p>The consumer group {@code gateway-consumer-group} receives messages routed
 * to Kafka by the dispatcher. It measures end-to-end latency (envelope creation
 * to consumer processing) and maintains running counters for the experiment runner.</p>
 *
 * <p>Manual acknowledgement mode is used to ensure at-least-once delivery
 * semantics and to give the consumer control over offset commits.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageConsumer {

    private final ObjectMapper objectMapper;

    private final AtomicLong consumedCount      = new AtomicLong(0);
    private final AtomicLong totalLatencyMicros = new AtomicLong(0);

    /**
     * Listens on all topics matching the {@code adaptive-*} wildcard pattern.
     * Spring Kafka uses a {@code TopicPartitionOffset} resolver when the topic
     * list uses the {@code ^} regex prefix.
     */
    @KafkaListener(
            topicPattern = "adaptive-.*",
            groupId      = "gateway-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            MessageEnvelope envelope = objectMapper.readValue(record.value(), MessageEnvelope.class);

            long latencyMicros = computeLatencyMicros(envelope.getCreatedAt());
            consumedCount.incrementAndGet();
            totalLatencyMicros.addAndGet(latencyMicros);

            log.debug("kafka-consumed: msg={} type={} topic={} partition={} offset={} latency={}µs",
                    envelope.getMessageId(),
                    envelope.getMessageType(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    latencyMicros);

            ack.acknowledge();

        } catch (Exception ex) {
            log.error("Failed to deserialise Kafka message at offset={} partition={}: {}",
                    record.offset(), record.partition(), ex.getMessage());
            // Acknowledge to avoid poison-pill reprocessing loop
            ack.acknowledge();
        }
    }

    /** Returns total messages consumed from Kafka since service start. */
    public long getConsumedCount() { return consumedCount.get(); }

    /** Returns average end-to-end latency in microseconds (0 if no messages consumed). */
    public long getAverageLatencyMicros() {
        long count = consumedCount.get();
        return count == 0 ? 0 : totalLatencyMicros.get() / count;
    }

    /** Resets counters — called between experiment runs. */
    public void resetCounters() {
        consumedCount.set(0);
        totalLatencyMicros.set(0);
    }

    private static long computeLatencyMicros(Instant createdAt) {
        return (System.currentTimeMillis() - createdAt.toEpochMilli()) * 1_000;
    }
}
