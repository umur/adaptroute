package com.umurinan.adaptroute.gateway.health;

import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.metrics.BrokerLoadMonitor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Composite health indicator that probes both Kafka and RabbitMQ.
 *
 * <p>Exposed at {@code /actuator/health/brokers}. Kubernetes liveness and readiness
 * probes should use {@code /actuator/health} which aggregates all indicators.
 * The routing infrastructure degrades gracefully when one broker is unhealthy —
 * the load monitor marks it as unhealthy and the scoring model assigns it a
 * load factor of 0.0, effectively routing all traffic to the surviving broker.</p>
 *
 * <p>Health probes run on two cadences:</p>
 * <ul>
 *   <li>{@link #scheduledHealthProbe()} — every 500 ms, with a 500 ms Kafka timeout,
 *       keeping {@link BrokerLoadMonitor} health flags current at the same rate as
 *       the load sampling interval. This is the primary update path for routing.</li>
 *   <li>{@link #health()} — on-demand when the actuator endpoint is hit, with a
 *       3 s Kafka timeout, returning rich detail for monitoring dashboards.</li>
 * </ul>
 */
@Slf4j
@Component("brokers")
@RequiredArgsConstructor
public class BrokerHealthIndicator implements HealthIndicator {

    private final AdminClient kafkaAdminClient;
    private final ConnectionFactory rabbitConnectionFactory;
    private final BrokerLoadMonitor loadMonitor;

    /** Kafka probe timeout for the actuator endpoint (generous; not on the routing path). */
    private static final long KAFKA_PROBE_TIMEOUT_MS = 3_000;

    /**
     * Kafka probe timeout for the 500 ms scheduled probe.
     * Must be shorter than the probe interval to avoid scheduler thread starvation.
     */
    private static final long KAFKA_SCHEDULED_PROBE_TIMEOUT_MS = 500;

    // =========================================================================
    // Scheduled probe — runs every 500 ms to keep health flags current
    // =========================================================================

    /**
     * Probes both brokers every 500 ms and updates the {@link BrokerLoadMonitor}
     * health flags. This ensures the scoring model reacts to broker failures within
     * one load-sampling interval (500 ms), matching the detection-window claim
     * in the paper.
     */
    @Scheduled(fixedDelay = 500)
    public void scheduledHealthProbe() {
        boolean kafkaOk  = probeKafkaWithTimeout(KAFKA_SCHEDULED_PROBE_TIMEOUT_MS);
        boolean rabbitOk = probeRabbitMq();
        loadMonitor.setKafkaHealthy(kafkaOk);
        loadMonitor.setRabbitHealthy(rabbitOk);
        log.debug("Scheduled health probe: kafka={} rabbit={}", kafkaOk, rabbitOk);
    }

    // =========================================================================
    // Actuator endpoint — on-demand, returns rich detail
    // =========================================================================

    @Override
    public Health health() {
        boolean kafkaOk  = probeKafka();
        boolean rabbitOk = probeRabbitMq();

        // Update load monitor so the scoring model reflects current health
        loadMonitor.setKafkaHealthy(kafkaOk);
        loadMonitor.setRabbitHealthy(rabbitOk);

        BrokerLoad kafkaLoad  = loadMonitor.getKafkaLoad();
        BrokerLoad rabbitLoad = loadMonitor.getRabbitMqLoad();

        Health.Builder builder = (kafkaOk || rabbitOk) ? Health.up() : Health.down();

        return builder.withDetails(Map.of(
                "kafka",    Map.of(
                        "healthy",       kafkaOk,
                        "loadScore",     String.format("%.3f", kafkaLoad.computeLoadScore()),
                        "msgPerSec",     String.format("%.1f", kafkaLoad.getMessagesPerSecond()),
                        "consumerLag",   kafkaLoad.getConsumerLag()
                ),
                "rabbitmq", Map.of(
                        "healthy",       rabbitOk,
                        "loadScore",     String.format("%.3f", rabbitLoad.computeLoadScore()),
                        "msgPerSec",     String.format("%.1f", rabbitLoad.getMessagesPerSecond()),
                        "consumerLag",   rabbitLoad.getConsumerLag()
                )
        )).build();
    }

    // =========================================================================
    // Probe implementations
    // =========================================================================

    private boolean probeKafka() {
        return probeKafkaWithTimeout(KAFKA_PROBE_TIMEOUT_MS);
    }

    private boolean probeKafkaWithTimeout(long timeoutMs) {
        try {
            kafkaAdminClient.listTopics()
                    .names()
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private boolean probeRabbitMq() {
        try {
            var connection = rabbitConnectionFactory.createConnection();
            boolean ok = connection.isOpen();
            connection.close();
            return ok;
        } catch (Exception ex) {
            return false;
        }
    }
}
