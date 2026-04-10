package com.umurinan.adaptroute.common.router;

import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.DeliveryGuarantee;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.MessagePriority;
import com.umurinan.adaptroute.common.dto.RoutingDecision;
import com.umurinan.adaptroute.common.dto.RoutingDecision.KafkaFactors;
import com.umurinan.adaptroute.common.dto.RoutingDecision.RabbitMqFactors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Weighted multi-criteria scoring router — the core contribution of this research.
 *
 * <h2>Scoring Model</h2>
 *
 * <p>For each incoming message, the router computes a composite affinity score for
 * Apache Kafka and for RabbitMQ independently, then dispatches to the broker with
 * the higher score:</p>
 *
 * <pre>
 *   score_kafka  = W_THROUGHPUT · throughputFactor
 *                + W_SIZE       · sizeFactor
 *                + W_ORDERING   · orderingFactor
 *                + W_LOAD       · loadFactor_kafka
 *                + W_DELIVERY   · deliveryGuaranteeFactor_kafka
 *
 *   score_rabbit = W_LATENCY    · latencyFactor
 *                + W_ROUTING    · routingFlexibilityFactor
 *                + W_ACK        · ackSpeedFactor
 *                + W_LOAD       · loadFactor_rabbit
 *                + W_DELIVERY   · deliveryGuaranteeFactor_rabbit
 *
 *   broker* = argmax(score_kafka, score_rabbit)
 * </pre>
 *
 * <p>All factor values are normalised to [0.0, 1.0] before weighting.
 * All weight vectors sum to 1.0 within their respective score expressions.</p>
 *
 * <h2>Factor Definitions</h2>
 *
 * <table border="1">
 *   <tr><th>Factor</th><th>Broker</th><th>Description</th></tr>
 *   <tr><td>throughputFactor</td><td>Kafka</td>
 *       <td>Sigmoid of message rate relative to THROUGHPUT_KNEE (msgs/s).</td></tr>
 *   <tr><td>sizeFactor</td><td>Kafka</td>
 *       <td>Sigmoid of payload size relative to SIZE_KNEE (bytes).
 *           Kafka's sequential log layout absorbs large payloads efficiently.</td></tr>
 *   <tr><td>orderingFactor</td><td>Kafka</td>
 *       <td>Binary: 1.0 when strict ordering is required, 0.0 otherwise.
 *           Kafka guarantees per-partition ordering; RabbitMQ does not.</td></tr>
 *   <tr><td>loadFactor</td><td>Both</td>
 *       <td>Inverse broker load from {@link BrokerLoad#computeLoadScore()}.
 *           Causes automatic traffic shedding when a broker approaches saturation.</td></tr>
 *   <tr><td>deliveryGuaranteeFactor</td><td>Both</td>
 *       <td>EXACTLY_ONCE maps to 1.0 for Kafka; AT_LEAST_ONCE / AT_MOST_ONCE
 *           maps to 1.0 for RabbitMQ.</td></tr>
 *   <tr><td>latencyFactor</td><td>RabbitMQ</td>
 *       <td>Priority-driven: CRITICAL=1.0, HIGH=0.75, NORMAL=0.4, LOW=0.1.
 *           RabbitMQ's push-model delivery achieves sub-millisecond latency.</td></tr>
 *   <tr><td>routingFlexibilityFactor</td><td>RabbitMQ</td>
 *       <td>1.0 for broadcast/fanout patterns (channel prefix "broadcast.*"),
 *           0.7 for topic patterns, 0.3 for direct point-to-point.</td></tr>
 *   <tr><td>ackSpeedFactor</td><td>RabbitMQ</td>
 *       <td>Complement of payload size factor: small messages confirm faster.</td></tr>
 * </table>
 *
 * <h2>Weight Calibration</h2>
 *
 * <p>Default weights were calibrated via grid-search on a synthetic workload
 * benchmark. They are externalised as Spring configuration properties so that
 * the paper's sensitivity analysis can be reproduced by changing
 * {@code router.weights.*} in {@code application.yml} without recompilation.</p>
 */
@Slf4j
@Component
public class WeightedScoringRouter implements MessageRouter {

    // =========================================================================
    // Kafka weight vector  (must sum to 1.0)
    // =========================================================================

    /** Weight for throughput affinity in Kafka score. */
    @Value("${router.weights.kafka.throughput:0.30}")
    private double wKThroughput;

    /** Weight for payload-size affinity in Kafka score. */
    @Value("${router.weights.kafka.size:0.20}")
    private double wKSize;

    /** Weight for ordering requirement in Kafka score. */
    @Value("${router.weights.kafka.ordering:0.20}")
    private double wKOrdering;

    /** Weight for broker load availability in Kafka score. */
    @Value("${router.weights.kafka.load:0.15}")
    private double wKLoad;

    /** Weight for delivery-guarantee affinity in Kafka score. */
    @Value("${router.weights.kafka.delivery:0.15}")
    private double wKDelivery;

    // =========================================================================
    // RabbitMQ weight vector  (must sum to 1.0)
    // =========================================================================

    /** Weight for low-latency affinity in RabbitMQ score. */
    @Value("${router.weights.rabbit.latency:0.30}")
    private double wRLatency;

    /** Weight for routing flexibility (exchange type) in RabbitMQ score. */
    @Value("${router.weights.rabbit.routing:0.20}")
    private double wRRouting;

    /** Weight for publisher-confirm (ack) speed in RabbitMQ score. */
    @Value("${router.weights.rabbit.ack:0.15}")
    private double wRAck;

    /** Weight for broker load availability in RabbitMQ score. */
    @Value("${router.weights.rabbit.load:0.20}")
    private double wRLoad;

    /** Weight for delivery-guarantee affinity in RabbitMQ score. */
    @Value("${router.weights.rabbit.delivery:0.15}")
    private double wRDelivery;

    // =========================================================================
    // Normalisation knees (sigmoid inflection points)
    // =========================================================================

    /**
     * Throughput knee: message rate at which Kafka affinity reaches the sigmoid
     * midpoint (factor ≈ 0.5). Below this rate RabbitMQ is preferred for latency;
     * above it Kafka is preferred for throughput.
     * Default: 500 messages/second.
     */
    private static final double THROUGHPUT_KNEE_MSG_PER_SEC = 500.0;

    /**
     * Size knee: payload size at which Kafka's sequential-write advantage becomes
     * dominant. Below 8 KB RabbitMQ's memory-resident queues are competitive.
     * Default: 8 192 bytes (8 KB).
     */
    private static final double SIZE_KNEE_BYTES = 8_192.0;

    /**
     * Sigmoid steepness parameter. Higher values produce a sharper transition
     * between low- and high-affinity regions.
     */
    private static final double SIGMOID_STEEPNESS = 5.0;

    // =========================================================================
    // MessageRouter implementation
    // =========================================================================

    /**
     * {@inheritDoc}
     *
     * <p>Processing steps:</p>
     * <ol>
     *   <li>If a {@link BrokerTarget#KAFKA} or {@link BrokerTarget#RABBITMQ} routing
     *       hint is set on the envelope, bypass scoring and return immediately
     *       (used for baseline experiments).</li>
     *   <li>Compute per-factor normalised scores for each broker.</li>
     *   <li>Apply the weight vectors to form composite scores.</li>
     *   <li>Select the broker with the higher composite score; break ties with Kafka.</li>
     *   <li>Build and return a fully documented {@link RoutingDecision}.</li>
     * </ol>
     */
    @Override
    public RoutingDecision route(MessageEnvelope envelope,
                                 BrokerLoad kafkaLoad,
                                 BrokerLoad rabbitMqLoad) {

        long startNanos = System.nanoTime();

        // ------------------------------------------------------------------
        // Step 1 — honour explicit routing hints (baseline experiment mode)
        // ------------------------------------------------------------------
        if (envelope.getRoutingHint() != null
                && envelope.getRoutingHint() != BrokerTarget.ADAPTIVE) {

            BrokerTarget forced = envelope.getRoutingHint();
            long elapsedMicros = (System.nanoTime() - startNanos) / 1_000;

            log.debug("msg={} routing-hint={} bypassing scoring model",
                    envelope.getMessageId(), forced);

            return RoutingDecision.builder()
                    .messageId(envelope.getMessageId())
                    .messageType(envelope.getMessageType())
                    .selectedBroker(forced)
                    .kafkaScore(0.0)
                    .rabbitMqScore(0.0)
                    .scoreMargin(0.0)
                    .kafkaFactors(zeroKafkaFactors())
                    .rabbitMqFactors(zeroRabbitMqFactors())
                    .routingHintApplied(true)
                    .decisionLatencyMicros(elapsedMicros)
                    .build();
        }

        // ------------------------------------------------------------------
        // Step 1b — enforce ordering constraint (hard rule, not a score factor)
        // ------------------------------------------------------------------
        // RabbitMQ provides no per-partition ordering guarantee. Any message
        // with orderingRequired=true must go to Kafka regardless of load or
        // latency scores. We still compute scores for observability but
        // override the selection unconditionally.
        if (envelope.isOrderingRequired()) {
            KafkaFactors kf = computeKafkaFactors(envelope, kafkaLoad);
            RabbitMqFactors rf = computeRabbitMqFactors(envelope, rabbitMqLoad);

            double kafkaScore = wKThroughput * kf.getThroughputFactor()
                    + wKSize       * kf.getSizeFactor()
                    + wKOrdering   * kf.getOrderingFactor()
                    + wKLoad       * kf.getLoadFactor()
                    + wKDelivery   * kf.getDeliveryGuaranteeFactor();

            double rabbitScore = wRLatency   * rf.getLatencyFactor()
                    + wRRouting    * rf.getRoutingFlexibilityFactor()
                    + wRAck        * rf.getAckSpeedFactor()
                    + wRLoad       * rf.getLoadFactor()
                    + wRDelivery   * rf.getDeliveryGuaranteeFactor();

            long elapsedMicros = (System.nanoTime() - startNanos) / 1_000;

            log.debug("msg={} type={} ordering-constraint: kafka={:.4f} rabbit={:.4f} -> KAFKA (forced)",
                    envelope.getMessageId(), envelope.getMessageType(), kafkaScore, rabbitScore);

            return RoutingDecision.builder()
                    .messageId(envelope.getMessageId())
                    .messageType(envelope.getMessageType())
                    .selectedBroker(BrokerTarget.KAFKA)
                    .kafkaScore(kafkaScore)
                    .rabbitMqScore(rabbitScore)
                    .scoreMargin(Math.abs(kafkaScore - rabbitScore))
                    .kafkaFactors(kf)
                    .rabbitMqFactors(rf)
                    .routingHintApplied(false)
                    .decisionLatencyMicros(elapsedMicros)
                    .build();
        }

        // ------------------------------------------------------------------
        // Step 2 — compute per-factor scores
        // ------------------------------------------------------------------
        KafkaFactors kf = computeKafkaFactors(envelope, kafkaLoad);
        RabbitMqFactors rf = computeRabbitMqFactors(envelope, rabbitMqLoad);

        // ------------------------------------------------------------------
        // Step 3 — apply weight vectors
        // ------------------------------------------------------------------
        double kafkaScore = wKThroughput * kf.getThroughputFactor()
                + wKSize       * kf.getSizeFactor()
                + wKOrdering   * kf.getOrderingFactor()
                + wKLoad       * kf.getLoadFactor()
                + wKDelivery   * kf.getDeliveryGuaranteeFactor();

        double rabbitScore = wRLatency   * rf.getLatencyFactor()
                + wRRouting    * rf.getRoutingFlexibilityFactor()
                + wRAck        * rf.getAckSpeedFactor()
                + wRLoad       * rf.getLoadFactor()
                + wRDelivery   * rf.getDeliveryGuaranteeFactor();

        // ------------------------------------------------------------------
        // Step 4 — select broker (ties go to Kafka for stability)
        // ------------------------------------------------------------------
        BrokerTarget selected = (kafkaScore >= rabbitScore)
                ? BrokerTarget.KAFKA
                : BrokerTarget.RABBITMQ;

        long elapsedMicros = (System.nanoTime() - startNanos) / 1_000;
        double margin = Math.abs(kafkaScore - rabbitScore);

        log.debug("msg={} type={} kafka={:.4f} rabbit={:.4f} margin={:.4f} -> {}",
                envelope.getMessageId(), envelope.getMessageType(),
                kafkaScore, rabbitScore, margin, selected);

        // ------------------------------------------------------------------
        // Step 5 — build and return decision
        // ------------------------------------------------------------------
        return RoutingDecision.builder()
                .messageId(envelope.getMessageId())
                .messageType(envelope.getMessageType())
                .selectedBroker(selected)
                .kafkaScore(kafkaScore)
                .rabbitMqScore(rabbitScore)
                .scoreMargin(margin)
                .kafkaFactors(kf)
                .rabbitMqFactors(rf)
                .routingHintApplied(false)
                .decisionLatencyMicros(elapsedMicros)
                .build();
    }

    // =========================================================================
    // Kafka factor computation
    // =========================================================================

    /**
     * Computes all normalised factors that contribute to the Kafka composite score.
     *
     * @param env       message envelope
     * @param kafkaLoad current Kafka cluster load snapshot
     * @return immutable factor bundle
     */
    private KafkaFactors computeKafkaFactors(MessageEnvelope env, BrokerLoad kafkaLoad) {

        // throughputFactor: sigmoid centred at THROUGHPUT_KNEE
        // Kafka's partitioned log provides near-linear throughput scaling.
        // We infer current stream throughput from the broker's messagesPerSecond counter.
        double throughputFactor = sigmoid(
                kafkaLoad.getMessagesPerSecond(), THROUGHPUT_KNEE_MSG_PER_SEC);

        // sizeFactor: sigmoid centred at SIZE_KNEE
        // Large payloads amortise Kafka's per-message protocol overhead efficiently.
        double sizeFactor = sigmoid(env.getPayloadSizeBytes(), SIZE_KNEE_BYTES);

        // orderingFactor: binary — Kafka's per-partition ordering guarantee is unique.
        double orderingFactor = env.isOrderingRequired() ? 1.0 : 0.0;

        // loadFactor: derived from BrokerLoad snapshot
        double loadFactor = kafkaLoad.computeLoadScore();

        // deliveryGuaranteeFactor: Kafka is the only broker supporting EXACTLY_ONCE
        // via the idempotent producer + transactional API.
        double deliveryGuaranteeFactor = switch (env.getDeliveryGuarantee()) {
            case EXACTLY_ONCE  -> 1.0;
            case AT_LEAST_ONCE -> 0.4;
            case AT_MOST_ONCE  -> 0.2;
        };

        return KafkaFactors.builder()
                .throughputFactor(throughputFactor)
                .sizeFactor(sizeFactor)
                .orderingFactor(orderingFactor)
                .loadFactor(loadFactor)
                .deliveryGuaranteeFactor(deliveryGuaranteeFactor)
                .build();
    }

    // =========================================================================
    // RabbitMQ factor computation
    // =========================================================================

    /**
     * Computes all normalised factors that contribute to the RabbitMQ composite score.
     *
     * @param env          message envelope
     * @param rabbitMqLoad current RabbitMQ broker load snapshot
     * @return immutable factor bundle
     */
    private RabbitMqFactors computeRabbitMqFactors(MessageEnvelope env,
                                                    BrokerLoad rabbitMqLoad) {

        // latencyFactor: driven by message priority.
        // RabbitMQ's push-delivery model achieves sub-millisecond consumer notification,
        // making it preferable for latency-sensitive messages.
        double latencyFactor = switch (env.getPriority()) {
            case CRITICAL -> 1.00;
            case HIGH     -> 0.75;
            case NORMAL   -> 0.40;
            case LOW      -> 0.10;
        };

        // routingFlexibilityFactor: RabbitMQ's exchange model supports sophisticated
        // routing topologies (fanout, topic, headers) not natively available in Kafka.
        double routingFlexibilityFactor = deriveRoutingFlexibility(env.getTargetChannel());

        // ackSpeedFactor: publisher confirms are faster for smaller messages
        // because they complete before hitting the TCP send-buffer ceiling.
        // We use the complement of the size sigmoid.
        double ackSpeedFactor = 1.0 - sigmoid(env.getPayloadSizeBytes(), SIZE_KNEE_BYTES);

        // loadFactor: same derivation as Kafka, but uses RabbitMQ's own snapshot.
        double loadFactor = rabbitMqLoad.computeLoadScore();

        // deliveryGuaranteeFactor: RabbitMQ excels at AT_LEAST_ONCE via publisher
        // confirms; AT_MOST_ONCE (fire-and-forget) is also its domain.
        double deliveryGuaranteeFactor = switch (env.getDeliveryGuarantee()) {
            case EXACTLY_ONCE  -> 0.1; // not natively supported
            case AT_LEAST_ONCE -> 1.0;
            case AT_MOST_ONCE  -> 0.8;
        };

        return RabbitMqFactors.builder()
                .latencyFactor(latencyFactor)
                .routingFlexibilityFactor(routingFlexibilityFactor)
                .ackSpeedFactor(ackSpeedFactor)
                .loadFactor(loadFactor)
                .deliveryGuaranteeFactor(deliveryGuaranteeFactor)
                .build();
    }

    // =========================================================================
    // Helper utilities
    // =========================================================================

    /**
     * Sigmoid normalisation function.
     *
     * <p>Maps an unbounded non-negative input value to [0.0, 1.0] with an
     * inflection at {@code knee}:</p>
     * <pre>
     *   sigmoid(x, k) = 1 / (1 + exp(-steepness * (x/k - 1)))
     * </pre>
     *
     * <p>At {@code x == knee} the output is exactly 0.5.
     * Below knee the score is below 0.5 (low affinity); above knee it approaches 1.0.</p>
     *
     * @param value    the raw metric value (e.g., payload size in bytes)
     * @param knee     the inflection point (e.g., {@link #SIZE_KNEE_BYTES})
     * @return normalised score in [0.0, 1.0]
     */
    static double sigmoid(double value, double knee) {
        if (knee <= 0) return 0.5;
        double exponent = -SIGMOID_STEEPNESS * ((value / knee) - 1.0);
        return 1.0 / (1.0 + Math.exp(exponent));
    }

    /**
     * Infers routing flexibility from the target channel name.
     *
     * <p>Naming conventions:</p>
     * <ul>
     *   <li>{@code broadcast.*} — fanout exchange; score 1.0</li>
     *   <li>{@code topic.*}     — topic exchange;  score 0.7</li>
     *   <li>anything else       — direct exchange; score 0.3</li>
     * </ul>
     */
    private static double deriveRoutingFlexibility(String targetChannel) {
        if (targetChannel == null) return 0.3;
        String lower = targetChannel.toLowerCase();
        if (lower.startsWith("broadcast.")) return 1.0;
        if (lower.startsWith("topic."))     return 0.7;
        return 0.3;
    }

    private static KafkaFactors zeroKafkaFactors() {
        return KafkaFactors.builder()
                .throughputFactor(0).sizeFactor(0).orderingFactor(0)
                .loadFactor(0).deliveryGuaranteeFactor(0).build();
    }

    private static RabbitMqFactors zeroRabbitMqFactors() {
        return RabbitMqFactors.builder()
                .latencyFactor(0).routingFlexibilityFactor(0).ackSpeedFactor(0)
                .loadFactor(0).deliveryGuaranteeFactor(0).build();
    }
}
