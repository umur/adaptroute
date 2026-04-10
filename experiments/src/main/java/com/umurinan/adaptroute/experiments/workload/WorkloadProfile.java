package com.umurinan.adaptroute.experiments.workload;

import lombok.Builder;
import lombok.Value;

/**
 * Immutable descriptor for a single experiment workload profile.
 *
 * <p>A workload profile defines the mix of message types, their emission rates,
 * payload size characteristics, and delivery semantics. The experiment runner
 * uses these profiles to generate reproducible, labelled workloads that
 * exercise different regions of the scoring model's decision space.</p>
 *
 * <h2>Profiles used in the paper</h2>
 * <ol>
 *   <li><strong>HIGH_THROUGHPUT_BURST</strong> — simulates a flash-sale event:
 *       thousands of order and analytics events per second, large payloads.
 *       Expected: adaptive router selects Kafka ≥ 90% of the time.</li>
 *   <li><strong>LOW_LATENCY_STREAM</strong> — simulates real-time alerting:
 *       high-priority notifications at moderate rate, small payloads.
 *       Expected: adaptive router selects RabbitMQ ≥ 85% of the time.</li>
 *   <li><strong>MIXED_WORKLOAD</strong> — equal mix of all three services.
 *       Expected: adaptive router splits intelligently based on per-message scoring.</li>
 *   <li><strong>ORDERING_CRITICAL</strong> — only order events with EXACTLY_ONCE
 *       semantics and strict ordering. Expected: Kafka 100% of the time.</li>
 * </ol>
 */
@Value
@Builder
public class WorkloadProfile {

    /** Human-readable identifier used in CSV output and chart labels. */
    String name;

    /** Description for paper and experiment log. */
    String description;

    /** Target total messages to send during this workload run. */
    int totalMessages;

    /** Fraction of total messages that are order events [0.0, 1.0]. */
    double orderFraction;

    /** Fraction of total messages that are notification events [0.0, 1.0]. */
    double notificationFraction;

    /** Fraction of total messages that are analytics events [0.0, 1.0]. */
    double analyticsFraction;

    /**
     * Target emit rate in messages/second across all services.
     * The workload generator paces emission to match this rate.
     */
    int targetMsgPerSecond;

    /**
     * Whether all order events in this profile require strict ordering.
     * When true, forces {@code orderingRequired = true} on every envelope.
     */
    boolean orderingCritical;

    /**
     * Fraction of messages that are marked CRITICAL or HIGH priority [0.0, 1.0].
     * Higher values boost RabbitMQ latency factor scores.
     */
    double highPriorityFraction;

    /**
     * Duration in seconds for which the workload runs.
     * Used when totalMessages is 0 (time-bounded rather than count-bounded run).
     */
    int durationSeconds;

    /**
     * When true, the generator alternates between burst and idle emission phases.
     * Only meaningful for W3-style mixed workloads.
     */
    boolean burstEnabled;

    /**
     * Peak emission rate during burst phase (msg/s). Ignored when burstEnabled=false.
     */
    int burstMsgPerSecond;

    /**
     * Trough emission rate during idle phase (msg/s). Ignored when burstEnabled=false.
     */
    int idleMsgPerSecond;

    /**
     * Minimum burst duration in seconds. Actual burst length is drawn uniformly
     * from [burstMinDurationSeconds, burstMaxDurationSeconds] per phase.
     */
    int burstMinDurationSeconds;

    /**
     * Maximum burst duration in seconds.
     */
    int burstMaxDurationSeconds;

    /**
     * Duration of each idle phase in seconds.
     */
    int idleDurationSeconds;

    // =========================================================================
    // Pre-built profile factories
    // =========================================================================

    public static WorkloadProfile highThroughputBurst() {
        return WorkloadProfile.builder()
                .name("HIGH_THROUGHPUT_BURST")
                .description("Flash-sale simulation: high-volume orders and analytics, large payloads. "
                        + "Exercises Kafka throughput and size factors.")
                .totalMessages(5_000)
                .orderFraction(0.40)
                .notificationFraction(0.10)
                .analyticsFraction(0.50)
                .targetMsgPerSecond(500)
                .orderingCritical(true)
                .highPriorityFraction(0.05)
                .durationSeconds(30)
                .build();
    }

    public static WorkloadProfile lowLatencyStream() {
        return WorkloadProfile.builder()
                .name("LOW_LATENCY_STREAM")
                .description("Real-time alerting simulation: high-priority notifications, small payloads. "
                        + "Exercises RabbitMQ latency and routing-flexibility factors.")
                .totalMessages(2_000)
                .orderFraction(0.05)
                .notificationFraction(0.90)
                .analyticsFraction(0.05)
                .targetMsgPerSecond(300)
                .orderingCritical(false)
                .highPriorityFraction(0.70)
                .durationSeconds(40)
                .build();
    }

    public static WorkloadProfile mixedWorkload() {
        return WorkloadProfile.builder()
                .name("MIXED_WORKLOAD")
                .description("Bursty mixed workload: equal type mix with alternating burst/idle phases. "
                        + "Burst rate 1500 msg/s for 60-120s, idle rate 200 msg/s for 30s. "
                        + "Exercises adaptive routing under runtime workload shifts.")
                .totalMessages(3_000)
                .orderFraction(0.33)
                .notificationFraction(0.34)
                .analyticsFraction(0.33)
                .targetMsgPerSecond(400)  // fallback rate (unused when burstEnabled=true)
                .orderingCritical(false)
                .highPriorityFraction(0.25)
                .durationSeconds(120)
                .burstEnabled(true)
                .burstMsgPerSecond(1_500)
                .idleMsgPerSecond(200)
                .burstMinDurationSeconds(60)
                .burstMaxDurationSeconds(120)
                .idleDurationSeconds(30)
                .build();
    }

    public static WorkloadProfile orderingCritical() {
        return WorkloadProfile.builder()
                .name("ORDERING_CRITICAL")
                .description("Pure order stream with EXACTLY_ONCE semantics and strict ordering. "
                        + "Expected result: Kafka selected for 100% of messages.")
                .totalMessages(1_000)
                .orderFraction(1.00)
                .notificationFraction(0.00)
                .analyticsFraction(0.00)
                .targetMsgPerSecond(200)
                .orderingCritical(true)
                .highPriorityFraction(0.00)
                .durationSeconds(35)
                .build();
    }
}
