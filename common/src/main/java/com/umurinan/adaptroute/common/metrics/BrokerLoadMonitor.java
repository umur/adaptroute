package com.umurinan.adaptroute.common.metrics;

import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Periodically samples broker load metrics and makes them available to the
 * {@link com.umurinan.adaptroute.common.router.WeightedScoringRouter} via lock-free
 * {@link AtomicReference} reads on the hot routing path.
 *
 * <h2>Sampling Strategy</h2>
 *
 * <p>Rather than making a blocking HTTP call inside every {@code route()} invocation,
 * a background {@link Scheduled} task refreshes the load snapshots every
 * {@link #SAMPLE_INTERVAL_MS} milliseconds. The router always reads the latest
 * cached snapshot. If a snapshot is older than {@link #STALE_THRESHOLD_SECONDS},
 * the monitor returns a {@link BrokerLoad#neutral(BrokerTarget) neutral} snapshot
 * to prevent stale data from influencing routing decisions.</p>
 *
 * <h2>Metric Sources</h2>
 * <ul>
 *   <li><strong>Kafka</strong>: consumer-group lag from the Kafka AdminClient
 *       {@code listConsumerGroupOffsets} API; throughput from internal message
 *       counters maintained by this class.</li>
 *   <li><strong>RabbitMQ</strong>: queue depth and unacked count from the
 *       RabbitMQ Management HTTP API ({@code /api/overview}).</li>
 * </ul>
 *
 * <p>In this research prototype, CPU and network utilisation are simulated via
 * a lightweight model that scales with observed throughput. A production deployment
 * would replace {@link #sampleKafkaLoad()} and {@link #sampleRabbitMqLoad()} with
 * real JMX / Management API calls.</p>
 */
@Slf4j
@Component
public class BrokerLoadMonitor {

    private static final long SAMPLE_INTERVAL_MS    = 500L;
    private static final long STALE_THRESHOLD_SECONDS = 30L;

    // Simulated operational ceiling for the synthetic load model
    private static final double MAX_SIMULATED_MSG_PER_SEC = 10_000.0;
    private static final long   MAX_SIMULATED_LAG         = 50_000L;

    // Atomic counters updated by the dispatcher on each successful send
    private final AtomicLong kafkaMessageCount  = new AtomicLong(0);
    private final AtomicLong rabbitMessageCount = new AtomicLong(0);
    private final AtomicLong kafkaLag           = new AtomicLong(0);
    private final AtomicLong rabbitLag          = new AtomicLong(0);

    // Rolling-window throughput trackers
    private volatile long lastKafkaCount  = 0;
    private volatile long lastRabbitCount = 0;
    private volatile long lastSampleTimeMs = System.currentTimeMillis();

    // Cached snapshots — written by the scheduler, read lock-free by the router
    private final AtomicReference<BrokerLoad> kafkaLoadRef =
            new AtomicReference<>(BrokerLoad.neutral(BrokerTarget.KAFKA));
    private final AtomicReference<BrokerLoad> rabbitLoadRef =
            new AtomicReference<>(BrokerLoad.neutral(BrokerTarget.RABBITMQ));

    // Health flags — set to false when a connectivity probe fails
    private volatile boolean kafkaHealthy  = true;
    private volatile boolean rabbitHealthy = true;

    // =========================================================================
    // Public API — called by the router on every message
    // =========================================================================

    /**
     * Returns the latest Kafka load snapshot.
     * If the snapshot is stale, returns a {@link BrokerLoad#neutral neutral} instance.
     */
    public BrokerLoad getKafkaLoad() {
        return guardStaleness(kafkaLoadRef.get(), BrokerTarget.KAFKA);
    }

    /**
     * Returns the latest RabbitMQ load snapshot.
     * If the snapshot is stale, returns a {@link BrokerLoad#neutral neutral} instance.
     */
    public BrokerLoad getRabbitMqLoad() {
        return guardStaleness(rabbitLoadRef.get(), BrokerTarget.RABBITMQ);
    }

    // =========================================================================
    // Counters updated by the dispatcher
    // =========================================================================

    /** Called by the Kafka dispatcher after each successful send. */
    public void incrementKafkaSent()  { kafkaMessageCount.incrementAndGet(); }

    /** Called by the RabbitMQ dispatcher after each successful send. */
    public void incrementRabbitSent() { rabbitMessageCount.incrementAndGet(); }

    /** Artificially injects consumer lag for simulation purposes. */
    public void setKafkaLag(long lag)  { kafkaLag.set(Math.max(0, lag)); }

    /** Artificially injects consumer lag for simulation purposes. */
    public void setRabbitLag(long lag) { rabbitLag.set(Math.max(0, lag)); }

    /** Updates Kafka health status (called by health-check probes). */
    public void setKafkaHealthy(boolean healthy)  { this.kafkaHealthy  = healthy; }

    /** Updates RabbitMQ health status (called by health-check probes). */
    public void setRabbitHealthy(boolean healthy) { this.rabbitHealthy = healthy; }

    /** Returns true if the Kafka broker is currently healthy. */
    public boolean isKafkaHealthy()  { return kafkaHealthy; }

    /** Returns true if the RabbitMQ broker is currently healthy. */
    public boolean isRabbitHealthy() { return rabbitHealthy; }

    // =========================================================================
    // Scheduled sampling
    // =========================================================================

    /**
     * Refreshes both broker load snapshots on a fixed-rate schedule.
     * Runs in the Spring task scheduler thread pool; never blocks the routing path.
     */
    @Scheduled(fixedDelay = SAMPLE_INTERVAL_MS)
    public void refreshLoadSnapshots() {
        try {
            long now = System.currentTimeMillis();
            long elapsedMs = Math.max(1, now - lastSampleTimeMs);

            // Compute rolling throughput (messages per second) since last sample
            long currentKafkaCount  = kafkaMessageCount.get();
            long currentRabbitCount = rabbitMessageCount.get();

            double kafkaMsgPerSec  = ((currentKafkaCount  - lastKafkaCount)  * 1000.0) / elapsedMs;
            double rabbitMsgPerSec = ((currentRabbitCount - lastRabbitCount) * 1000.0) / elapsedMs;

            lastKafkaCount  = currentKafkaCount;
            lastRabbitCount = currentRabbitCount;
            lastSampleTimeMs = now;

            kafkaLoadRef.set(sampleKafkaLoad(kafkaMsgPerSec));
            rabbitLoadRef.set(sampleRabbitMqLoad(rabbitMsgPerSec));

            log.debug("broker-load sampled: kafka={:.1f} msg/s rabbit={:.1f} msg/s",
                    kafkaMsgPerSec, rabbitMsgPerSec);

        } catch (Exception ex) {
            log.warn("Failed to refresh broker load snapshots: {}", ex.getMessage());
        }
    }

    // =========================================================================
    // Sampling implementations (prototype: synthetic model)
    // =========================================================================

    /**
     * Samples current Kafka cluster load.
     *
     * <p>In this prototype the CPU and network utilisation are derived from
     * observed throughput via a linear model capped at the simulated ceiling.
     * Replace this method with real AdminClient + JMX calls in production.</p>
     */
    private BrokerLoad sampleKafkaLoad(double msgPerSec) {
        double cpuUtilisation     = Math.min(msgPerSec / MAX_SIMULATED_MSG_PER_SEC, 1.0) * 0.7;
        double networkUtilisation = Math.min(msgPerSec / MAX_SIMULATED_MSG_PER_SEC, 1.0) * 0.8;

        return BrokerLoad.builder()
                .broker(BrokerTarget.KAFKA)
                .messagesPerSecond(msgPerSec)
                .consumerLag(kafkaLag.get())
                .cpuUtilisation(cpuUtilisation)
                .networkUtilisation(networkUtilisation)
                .healthy(kafkaHealthy)
                .sampledAt(Instant.now())
                .build();
    }

    /**
     * Samples current RabbitMQ broker load.
     *
     * <p>In this prototype, the model mirrors the Kafka approach.
     * Replace with RabbitMQ Management HTTP API calls ({@code GET /api/overview})
     * in production.</p>
     */
    private BrokerLoad sampleRabbitMqLoad(double msgPerSec) {
        double cpuUtilisation     = Math.min(msgPerSec / MAX_SIMULATED_MSG_PER_SEC, 1.0) * 0.65;
        double networkUtilisation = Math.min(msgPerSec / MAX_SIMULATED_MSG_PER_SEC, 1.0) * 0.75;

        return BrokerLoad.builder()
                .broker(BrokerTarget.RABBITMQ)
                .messagesPerSecond(msgPerSec)
                .consumerLag(rabbitLag.get())
                .cpuUtilisation(cpuUtilisation)
                .networkUtilisation(networkUtilisation)
                .healthy(rabbitHealthy)
                .sampledAt(Instant.now())
                .build();
    }

    // =========================================================================
    // Staleness guard
    // =========================================================================

    private BrokerLoad guardStaleness(BrokerLoad load, BrokerTarget broker) {
        if (load.getSampledAt().until(Instant.now(), ChronoUnit.SECONDS) > STALE_THRESHOLD_SECONDS) {
            log.warn("Stale load snapshot for {}; falling back to neutral", broker);
            return BrokerLoad.neutral(broker);
        }
        return load;
    }
}
