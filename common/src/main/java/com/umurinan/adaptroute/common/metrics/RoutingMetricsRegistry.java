package com.umurinan.adaptroute.common.metrics;

import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.RoutingDecision;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central Micrometer-based metrics registry for the adaptive routing system.
 *
 * <p>All routing-relevant measurements are registered here and exported via the
 * {@code /actuator/prometheus} endpoint in Prometheus text format. The Prometheus
 * scrape configuration in {@code docker-compose.yml} pulls these metrics every
 * 15 seconds for time-series storage and alerting.</p>
 *
 * <h2>Exported Metrics</h2>
 * <pre>
 *   adaptive_routing_decisions_total{broker, message_type}   counter
 *   adaptive_routing_score_margin_summary{selected_broker}   distribution summary
 *   adaptive_routing_decision_latency_micros{broker}         timer
 *   adaptive_routing_kafka_score_summary{message_type}       distribution summary
 *   adaptive_routing_rabbit_score_summary{message_type}      distribution summary
 *   adaptive_routing_messages_dispatched_total{broker}       counter
 *   adaptive_routing_dispatch_errors_total{broker}           counter
 *   adaptive_routing_payload_bytes{broker}                   distribution summary
 * </pre>
 */
@Slf4j
@Component
public class RoutingMetricsRegistry {

    private static final String PREFIX = "adaptive_routing";

    private final MeterRegistry registry;

    // Pre-built counters keyed by broker name to avoid per-call meter lookup overhead
    private final ConcurrentHashMap<String, Counter> dispatchCounters    = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> errorCounters       = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> latencyAccumulators = new ConcurrentHashMap<>();

    // Shared distribution summaries
    private final DistributionSummary scoreMarginSummary;
    private final DistributionSummary kafkaScoreSummary;
    private final DistributionSummary rabbitScoreSummary;
    private final DistributionSummary payloadBytesSummaryKafka;
    private final DistributionSummary payloadBytesSummaryRabbit;

    public RoutingMetricsRegistry(MeterRegistry registry) {
        this.registry = registry;

        this.scoreMarginSummary = DistributionSummary.builder(PREFIX + "_score_margin")
                .description("Absolute score margin between Kafka and RabbitMQ at routing time")
                .baseUnit("score_units")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(registry);

        this.kafkaScoreSummary = DistributionSummary.builder(PREFIX + "_kafka_score")
                .description("Composite Kafka score computed by the weighted scoring model")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(registry);

        this.rabbitScoreSummary = DistributionSummary.builder(PREFIX + "_rabbit_score")
                .description("Composite RabbitMQ score computed by the weighted scoring model")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(registry);

        this.payloadBytesSummaryKafka = DistributionSummary.builder(PREFIX + "_payload_bytes")
                .description("Payload sizes of messages dispatched to each broker")
                .baseUnit("bytes")
                .tag("broker", "kafka")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.payloadBytesSummaryRabbit = DistributionSummary.builder(PREFIX + "_payload_bytes")
                .description("Payload sizes of messages dispatched to each broker")
                .baseUnit("bytes")
                .tag("broker", "rabbitmq")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    /**
     * Records a completed routing decision.
     *
     * <p>Called immediately after the scoring model returns, before actual dispatch.
     * This ensures decisions are counted even if the subsequent broker send fails.</p>
     *
     * @param decision the routing decision produced by the scoring model
     * @param payloadSizeBytes byte length of the message payload
     */
    public void recordDecision(RoutingDecision decision, long payloadSizeBytes) {
        String broker = decision.getSelectedBroker().name().toLowerCase();
        String msgType = decision.getMessageType() != null ? decision.getMessageType() : "unknown";

        // Per-broker-per-type decision counter
        Counter.builder(PREFIX + "_decisions_total")
                .description("Total routing decisions made, broken down by target broker and message type")
                .tags(List.of(
                        Tag.of("broker", broker),
                        Tag.of("message_type", msgType),
                        Tag.of("hint_applied", String.valueOf(decision.isRoutingHintApplied()))
                ))
                .register(registry)
                .increment();

        // Score distribution summaries (only for adaptive decisions)
        if (!decision.isRoutingHintApplied()) {
            scoreMarginSummary.record(decision.getScoreMargin());
            kafkaScoreSummary.record(decision.getKafkaScore());
            rabbitScoreSummary.record(decision.getRabbitMqScore());
        }

        // Decision latency timer
        Timer.builder(PREFIX + "_decision_latency")
                .description("Time taken by the scoring model to produce a routing decision")
                .tag("broker", broker)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(Duration.ofNanos(decision.getDecisionLatencyMicros() * 1_000));

        // Payload size tracking
        if (decision.getSelectedBroker() == BrokerTarget.KAFKA) {
            payloadBytesSummaryKafka.record(payloadSizeBytes);
        } else {
            payloadBytesSummaryRabbit.record(payloadSizeBytes);
        }

        log.trace("metrics recorded: decision msg={} broker={} margin={:.4f}",
                decision.getMessageId(), broker, decision.getScoreMargin());
    }

    /**
     * Records a successful message dispatch to a broker.
     *
     * @param broker           target broker
     * @param latencyMicros    end-to-end dispatch latency from envelope creation to broker ack
     */
    public void recordDispatch(BrokerTarget broker, long latencyMicros) {
        String brokerName = broker.name().toLowerCase();
        dispatchCounters.computeIfAbsent(brokerName, b ->
                Counter.builder(PREFIX + "_messages_dispatched_total")
                        .description("Total messages successfully dispatched to each broker")
                        .tag("broker", b)
                        .register(registry)
        ).increment();

        Timer.builder(PREFIX + "_end_to_end_latency")
                .description("End-to-end message latency from envelope creation to broker acknowledgement")
                .tag("broker", brokerName)
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .register(registry)
                .record(Duration.ofNanos(latencyMicros * 1_000));
    }

    /**
     * Records a dispatch error.
     *
     * @param broker         target broker that rejected or timed out
     * @param errorType      short classification (e.g., "timeout", "connection_refused")
     */
    public void recordError(BrokerTarget broker, String errorType) {
        String brokerName = broker.name().toLowerCase();
        Counter.builder(PREFIX + "_dispatch_errors_total")
                .description("Total dispatch failures per broker and error type")
                .tags(List.of(
                        Tag.of("broker", brokerName),
                        Tag.of("error_type", errorType)
                ))
                .register(registry)
                .increment();

        log.warn("dispatch error recorded: broker={} type={}", brokerName, errorType);
    }

    /**
     * Records that a fallback routing was applied because the primary broker
     * was unavailable or overloaded.
     *
     * @param original  the broker originally selected by the scoring model
     * @param fallback  the broker actually used after fallback
     */
    public void recordFallback(BrokerTarget original, BrokerTarget fallback) {
        Counter.builder(PREFIX + "_fallback_total")
                .description("Number of times the router fell back to an alternative broker")
                .tags(List.of(
                        Tag.of("original_broker", original.name().toLowerCase()),
                        Tag.of("fallback_broker", fallback.name().toLowerCase())
                ))
                .register(registry)
                .increment();

        log.info("fallback applied: {} -> {}", original, fallback);
    }
}
