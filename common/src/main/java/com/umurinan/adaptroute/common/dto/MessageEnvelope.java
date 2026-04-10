package com.umurinan.adaptroute.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.annotation.JsonDeserialize;
import tools.jackson.databind.annotation.JsonPOJOBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Universal message container that wraps all payloads routed through the adaptive system.
 *
 * <p>Every message entering the routing infrastructure is first wrapped in a
 * {@code MessageEnvelope}. The envelope carries both the business payload and the
 * routing-relevant metadata (size, priority, delivery semantics) that the
 * {@link com.umurinan.adaptroute.common.router.MessageRouter} scoring model uses to
 * compute broker affinity scores.</p>
 *
 * <p>Immutability is enforced via Lombok {@code @Value}; the builder pattern allows
 * fluent construction without exposing mutable state.</p>
 */
@Value
@Builder
@JsonDeserialize(builder = MessageEnvelope.MessageEnvelopeBuilder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MessageEnvelope {

    /**
     * Globally unique identifier for this message instance.
     * Used for deduplication, idempotency checks, and distributed tracing correlation.
     */
    @Builder.Default
    String messageId = UUID.randomUUID().toString();

    /**
     * Logical type of the enclosed payload (e.g., "ORDER_CREATED", "USER_NOTIFICATION").
     * The router uses this to apply domain-specific routing overrides.
     */
    String messageType;

    /**
     * Originating microservice name (e.g., "order-service").
     * Useful for attribution in routing metrics and debugging.
     */
    String sourceService;

    /**
     * Target logical channel or topic name, independent of the physical broker.
     * The router resolves this to a Kafka topic or AMQP exchange at dispatch time.
     */
    String targetChannel;

    /**
     * The actual business payload serialised as a Jackson {@code JsonNode}.
     * Using a generic JSON node rather than a typed POJO keeps the common module
     * free of domain-specific dependencies.
     */
    JsonNode payload;

    /**
     * Pre-computed byte size of the serialised payload.
     * Computed once by the producer and carried in the envelope to avoid repeated
     * serialisation during scoring.
     */
    long payloadSizeBytes;

    /**
     * Message priority expressed as a {@link MessagePriority} enum value.
     * High-priority messages receive a latency bias in the scoring model.
     */
    @Builder.Default
    MessagePriority priority = MessagePriority.NORMAL;

    /**
     * Required delivery semantics declared by the producer.
     * Maps directly onto broker capabilities: EXACTLY_ONCE favours Kafka transactions,
     * AT_LEAST_ONCE favours RabbitMQ publisher confirms.
     */
    @Builder.Default
    DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;

    /**
     * Whether strict per-partition ordering must be preserved for this message stream.
     * When {@code true}, the scoring model applies a strong Kafka affinity bonus.
     */
    @Builder.Default
    boolean orderingRequired = false;

    /**
     * Wall-clock timestamp at the moment of envelope creation (producer side).
     * Together with consumer-side timestamps, this enables end-to-end latency measurement.
     */
    @Builder.Default
    Instant createdAt = Instant.now();

    /**
     * Arbitrary key-value metadata for extensibility.
     * Examples: trace IDs, schema version, tenant ID, experiment label.
     */
    Map<String, String> headers;

    /**
     * Routing hint set by the experiment runner to force a specific broker,
     * bypassing the adaptive scoring model. Used for baseline comparisons.
     * {@code null} means the adaptive router decides.
     */
    BrokerTarget routingHint;

    // -------------------------------------------------------------------------
    // Nested enumerations
    // -------------------------------------------------------------------------

    /**
     * Supported message broker targets.
     */
    public enum BrokerTarget {
        KAFKA,
        RABBITMQ,
        ADAPTIVE,     // let the scoring model decide
        STATIC_SPLIT  // route by message type: ORDER->Kafka, NOTIFICATION->RabbitMQ, ANALYTICS->hash split
    }

    /**
     * Delivery guarantee semantics supported by the routing infrastructure.
     */
    public enum DeliveryGuarantee {
        /** Best effort — no producer acknowledgement required. */
        AT_MOST_ONCE,
        /** Standard acknowledged delivery; duplicates possible on retry. */
        AT_LEAST_ONCE,
        /** Transactional, idempotent delivery; Kafka-exclusive. */
        EXACTLY_ONCE
    }

    /**
     * Coarse-grained priority classification influencing latency scoring.
     */
    public enum MessagePriority {
        LOW,
        NORMAL,
        HIGH,
        CRITICAL
    }

    /**
     * Lombok-generated builder — annotated for Jackson 3 deserialization.
     * The body is intentionally empty; Lombok fills in all setter methods.
     */
    @JsonPOJOBuilder(withPrefix = "")
    public static class MessageEnvelopeBuilder {
        // Lombok generates all builder methods
    }
}
