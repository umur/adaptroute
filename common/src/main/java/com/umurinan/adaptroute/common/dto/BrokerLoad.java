package com.umurinan.adaptroute.common.dto;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Point-in-time snapshot of a message broker's operational load.
 *
 * <p>The adaptive scoring model uses these metrics to compute the {@code loadFactor}
 * for each broker. When a broker approaches capacity saturation, its load factor
 * score decreases, causing the router to shift traffic to the complementary broker
 * — providing automatic load-based failover without explicit circuit-breaker logic.</p>
 *
 * <p>Instances are produced by {@link com.umurinan.adaptroute.common.metrics.BrokerLoadMonitor}
 * at a configurable sampling interval (default 5 seconds) and cached in an
 * {@code AtomicReference} for lock-free reads during the hot routing path.</p>
 */
@Value
@Builder
public class BrokerLoad {

    /** Broker identity. */
    MessageEnvelope.BrokerTarget broker;

    /**
     * Current message production rate in messages/second.
     * Measured as a rolling average over the last sampling window.
     */
    double messagesPerSecond;

    /**
     * Consumer lag: number of messages awaiting processing.
     * For Kafka this is the sum of per-partition consumer group lag.
     * For RabbitMQ this is the sum of unacked messages across monitored queues.
     */
    long consumerLag;

    /**
     * Broker CPU utilisation as a fraction [0.0, 1.0].
     * Sourced from JMX (Kafka) or the RabbitMQ Management HTTP API.
     */
    double cpuUtilisation;

    /**
     * Network I/O utilisation as a fraction [0.0, 1.0].
     * High network utilisation is a strong signal that the broker is approaching
     * its throughput ceiling.
     */
    double networkUtilisation;

    /**
     * Whether the broker health check is currently passing.
     * A {@code false} value triggers immediate routing away from this broker
     * regardless of the numerical score.
     */
    @Builder.Default
    boolean healthy = true;

    /** Timestamp of this snapshot. Used to detect stale readings. */
    @Builder.Default
    Instant sampledAt = Instant.now();

    /**
     * Computes a normalised load score in the range [0.0, 1.0].
     *
     * <p>A score of {@code 1.0} means the broker is completely idle (zero load).
     * A score of {@code 0.0} means the broker is saturated or unhealthy and
     * should not receive additional traffic.</p>
     *
     * <p>The formula combines CPU, network, and a lag penalty:</p>
     * <pre>
     *   lagPenalty  = min(consumerLag / LAG_CEILING, 1.0)
     *   rawLoad     = 0.4 * cpuUtilisation
     *               + 0.4 * networkUtilisation
     *               + 0.2 * lagPenalty
     *   loadScore   = 1.0 - rawLoad          (if healthy)
     *               = 0.0                    (if unhealthy)
     * </pre>
     *
     * @return normalised availability score, higher is better
     */
    public double computeLoadScore() {
        if (!healthy) {
            return 0.0;
        }

        final double lagCeiling = 100_000.0; // messages; tunable
        double lagPenalty = Math.min((double) consumerLag / lagCeiling, 1.0);

        double rawLoad = 0.4 * cpuUtilisation
                + 0.4 * networkUtilisation
                + 0.2 * lagPenalty;

        return Math.max(0.0, 1.0 - rawLoad);
    }

    /**
     * Returns a zero-load, healthy placeholder used before the first real
     * broker health snapshot is available (avoids null checks in the router).
     */
    public static BrokerLoad neutral(MessageEnvelope.BrokerTarget broker) {
        return BrokerLoad.builder()
                .broker(broker)
                .messagesPerSecond(0)
                .consumerLag(0)
                .cpuUtilisation(0.0)
                .networkUtilisation(0.0)
                .healthy(true)
                .build();
    }
}
