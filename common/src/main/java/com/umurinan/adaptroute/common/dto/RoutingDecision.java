package com.umurinan.adaptroute.common.dto;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Immutable record of a single routing decision produced by the scoring model.
 *
 * <p>Every message dispatched through the adaptive router generates exactly one
 * {@code RoutingDecision}. The decision captures not only the chosen broker but
 * also the full scoring breakdown, enabling post-hoc analysis and paper-level
 * reproducibility.</p>
 *
 * <p>Instances are emitted to Micrometer metrics and written to CSV by the
 * experiment runner for statistical evaluation.</p>
 */
@Value
@Builder
public class RoutingDecision {

    /** The message this decision was made for. */
    String messageId;

    /** The message type for grouping in analysis. */
    String messageType;

    /** Broker selected by the scoring model (or forced via routing hint). */
    MessageEnvelope.BrokerTarget selectedBroker;

    /** Raw composite score for Kafka (higher = stronger Kafka affinity). */
    double kafkaScore;

    /** Raw composite score for RabbitMQ (higher = stronger RabbitMQ affinity). */
    double rabbitMqScore;

    /** Score margin between the two brokers: {@code |kafkaScore - rabbitMqScore|}. */
    double scoreMargin;

    /** Individual factor scores contributing to the Kafka composite score. */
    KafkaFactors kafkaFactors;

    /** Individual factor scores contributing to the RabbitMQ composite score. */
    RabbitMqFactors rabbitMqFactors;

    /** Whether this decision was overridden by a routing hint (baseline mode). */
    boolean routingHintApplied;

    /** Time taken to compute the routing decision, in microseconds. */
    long decisionLatencyMicros;

    /** Wall-clock timestamp when the decision was made. */
    @Builder.Default
    Instant decidedAt = Instant.now();

    // -------------------------------------------------------------------------
    // Factor breakdown records — used for metrics and CSV export
    // -------------------------------------------------------------------------

    /**
     * Decomposed Kafka scoring factors.
     *
     * <pre>
     * score_kafka = w1*throughputFactor + w2*sizeFactor
     *             + w3*orderingFactor   + w4*loadFactor
     * </pre>
     */
    @Value
    @Builder
    public static class KafkaFactors {
        /** Normalised throughput affinity [0.0, 1.0]. High-volume streams score near 1.0. */
        double throughputFactor;
        /** Normalised payload-size affinity [0.0, 1.0]. Large payloads score near 1.0. */
        double sizeFactor;
        /** Ordering affinity [0.0, 1.0]. 1.0 when ordering is required. */
        double orderingFactor;
        /** Inverse of current Kafka broker load [0.0, 1.0]. 0.0 = fully loaded. */
        double loadFactor;
        /** Exactly-once delivery affinity [0.0, 1.0]. */
        double deliveryGuaranteeFactor;
    }

    /**
     * Decomposed RabbitMQ scoring factors.
     *
     * <pre>
     * score_rabbit = w1*latencyFactor + w2*routingFlexibilityFactor
     *              + w3*ackSpeedFactor + w4*loadFactor
     * </pre>
     */
    @Value
    @Builder
    public static class RabbitMqFactors {
        /** Normalised low-latency affinity [0.0, 1.0]. Critical-priority messages score near 1.0. */
        double latencyFactor;
        /** Routing flexibility affinity [0.0, 1.0]. Fanout/topic exchanges score near 1.0. */
        double routingFlexibilityFactor;
        /** Publisher confirm (ack) speed affinity [0.0, 1.0]. */
        double ackSpeedFactor;
        /** Inverse of current RabbitMQ broker load [0.0, 1.0]. 0.0 = fully loaded. */
        double loadFactor;
        /** At-least-once delivery affinity [0.0, 1.0]. */
        double deliveryGuaranteeFactor;
    }
}
