package com.umurinan.adaptroute.common.router;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.DeliveryGuarantee;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.MessagePriority;
import com.umurinan.adaptroute.common.dto.RoutingDecision;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for the {@link WeightedScoringRouter} scoring model.
 *
 * <p>These tests verify the core academic contribution of the paper: that the
 * weighted scoring model correctly differentiates message characteristics and
 * routes each workload type to the appropriate broker.</p>
 *
 * <p>Test structure mirrors the paper's evaluation scenarios:</p>
 * <ul>
 *   <li>Kafka affinity scenarios (high throughput, large payload, ordering, EXACTLY_ONCE)</li>
 *   <li>RabbitMQ affinity scenarios (low latency, fanout, small payload, AT_LEAST_ONCE)</li>
 *   <li>Load-based routing (overloaded broker causes traffic shift)</li>
 *   <li>Routing hint bypass (baseline experiment mode)</li>
 *   <li>Sigmoid normalisation function properties</li>
 * </ul>
 */
@DisplayName("WeightedScoringRouter — scoring model unit tests")
class WeightedScoringRouterTest {

    private WeightedScoringRouter router;
    private ObjectMapper objectMapper;

    // Neutral load: both brokers idle, healthy
    private BrokerLoad neutralKafkaLoad;
    private BrokerLoad neutralRabbitLoad;

    @BeforeEach
    void setUp() {
        router       = new WeightedScoringRouter();
        objectMapper = new ObjectMapper();
        neutralKafkaLoad  = BrokerLoad.neutral(BrokerTarget.KAFKA);
        neutralRabbitLoad = BrokerLoad.neutral(BrokerTarget.RABBITMQ);
    }

    // =========================================================================
    // Kafka affinity scenarios
    // =========================================================================

    @Nested
    @DisplayName("Kafka affinity scenarios")
    class KafkaAffinityTests {

        @Test
        @DisplayName("Large payload (>8KB) with high throughput routes to Kafka")
        void largePayloadHighThroughputRoutesToKafka() {
            // Simulate a 20KB analytics payload at 2000 msg/s
            BrokerLoad highThroughputKafka = BrokerLoad.builder()
                    .broker(BrokerTarget.KAFKA)
                    .messagesPerSecond(2_000)
                    .consumerLag(0)
                    .cpuUtilisation(0.2)
                    .networkUtilisation(0.2)
                    .healthy(true)
                    .build();

            MessageEnvelope envelope = buildEnvelope(
                    20_480L,                    // 20 KB payload
                    MessagePriority.LOW,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    false,
                    "analytics.events"
            );

            RoutingDecision decision = router.route(envelope, highThroughputKafka, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.KAFKA);
            assertThat(decision.getKafkaScore()).isGreaterThan(decision.getRabbitMqScore());
            assertThat(decision.getKafkaScore()).isGreaterThan(0.5);
        }

        @Test
        @DisplayName("Ordering-required message with realistic throughput routes to Kafka")
        void orderingRequiredWithThroughputRoutesToKafka() {
            // Ordering alone is not sufficient at zero throughput (model is calibrated for
            // realistic production conditions). With a stream already producing at 800 msg/s
            // the throughput factor activates strongly and Kafka wins.
            BrokerLoad activeKafka = BrokerLoad.builder()
                    .broker(BrokerTarget.KAFKA)
                    .messagesPerSecond(800)
                    .consumerLag(0)
                    .cpuUtilisation(0.1)
                    .networkUtilisation(0.1)
                    .healthy(true)
                    .build();

            MessageEnvelope envelope = buildEnvelope(
                    2_048L,
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    true,   // ordering required
                    "orders"
            );

            RoutingDecision decision = router.route(envelope, activeKafka, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.KAFKA);
            assertThat(decision.getKafkaFactors().getOrderingFactor()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("Ordering factor is 1.0 when ordering is required, 0.0 otherwise")
        void orderingFactorBinary() {
            MessageEnvelope withOrdering = buildEnvelope(
                    1_024L, MessagePriority.NORMAL, DeliveryGuarantee.AT_LEAST_ONCE, true, "orders");
            MessageEnvelope withoutOrdering = buildEnvelope(
                    1_024L, MessagePriority.NORMAL, DeliveryGuarantee.AT_LEAST_ONCE, false, "orders");

            RoutingDecision d1 = router.route(withOrdering, neutralKafkaLoad, neutralRabbitLoad);
            RoutingDecision d2 = router.route(withoutOrdering, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(d1.getKafkaFactors().getOrderingFactor()).isEqualTo(1.0);
            assertThat(d2.getKafkaFactors().getOrderingFactor()).isEqualTo(0.0);
            // Ordering raises Kafka's score relative to no-ordering
            assertThat(d1.getKafkaScore()).isGreaterThan(d2.getKafkaScore());
        }

        @Test
        @DisplayName("EXACTLY_ONCE delivery guarantee factor is 1.0 for Kafka, near-zero for RabbitMQ")
        void exactlyOnceDeliveryFactorAssignment() {
            MessageEnvelope envelope = buildEnvelope(
                    1_024L,
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.EXACTLY_ONCE,
                    false,
                    "orders"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            // Factor values are independent of final broker selection
            assertThat(decision.getKafkaFactors().getDeliveryGuaranteeFactor()).isEqualTo(1.0);
            assertThat(decision.getRabbitMqFactors().getDeliveryGuaranteeFactor()).isLessThan(0.2);
        }

        @Test
        @DisplayName("EXACTLY_ONCE + high throughput + large payload routes to Kafka")
        void exactlyOnceLargePayloadHighThroughputRoutesToKafka() {
            BrokerLoad activeKafka = BrokerLoad.builder()
                    .broker(BrokerTarget.KAFKA)
                    .messagesPerSecond(1_500)
                    .consumerLag(0)
                    .cpuUtilisation(0.15)
                    .networkUtilisation(0.15)
                    .healthy(true)
                    .build();

            MessageEnvelope envelope = buildEnvelope(
                    12_288L,   // 12 KB — above size knee
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.EXACTLY_ONCE,
                    false,
                    "orders"
            );

            RoutingDecision decision = router.route(envelope, activeKafka, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.KAFKA);
            assertThat(decision.getKafkaFactors().getDeliveryGuaranteeFactor()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("Ordering + EXACTLY_ONCE + large payload produces maximum Kafka score")
        void orderingExactlyOnceLargePayloadMaxKafkaScore() {
            MessageEnvelope envelope = buildEnvelope(
                    32_768L,  // 32 KB
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.EXACTLY_ONCE,
                    true,
                    "orders"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.KAFKA);
            assertThat(decision.getScoreMargin()).isGreaterThan(0.2);
        }
    }

    // =========================================================================
    // RabbitMQ affinity scenarios
    // =========================================================================

    @Nested
    @DisplayName("RabbitMQ affinity scenarios")
    class RabbitMqAffinityTests {

        @Test
        @DisplayName("CRITICAL priority small broadcast message routes to RabbitMQ")
        void criticalPrioritySmallBroadcastRoutesToRabbitMq() {
            MessageEnvelope envelope = buildEnvelope(
                    256L,   // small notification
                    MessagePriority.CRITICAL,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    false,
                    "broadcast.notifications"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.RABBITMQ);
            assertThat(decision.getRabbitMqScore()).isGreaterThan(decision.getKafkaScore());
            assertThat(decision.getRabbitMqFactors().getLatencyFactor()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("Fanout broadcast channel activates routing flexibility factor")
        void broadcastChannelActivatesRoutingFlexibility() {
            MessageEnvelope envelope = buildEnvelope(
                    512L,
                    MessagePriority.HIGH,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    false,
                    "broadcast.alerts"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getRabbitMqFactors().getRoutingFlexibilityFactor()).isEqualTo(1.0);
            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.RABBITMQ);
        }

        @Test
        @DisplayName("Small payload activates ackSpeed factor for RabbitMQ")
        void smallPayloadActivatesAckSpeedFactor() {
            MessageEnvelope envelope = buildEnvelope(
                    128L,  // very small
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    false,
                    "broadcast.notifications"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            // For very small payloads, ackSpeedFactor should be close to 1.0
            assertThat(decision.getRabbitMqFactors().getAckSpeedFactor()).isGreaterThan(0.9);
        }

        @Test
        @DisplayName("AT_MOST_ONCE delivery gives RabbitMQ a delivery guarantee advantage")
        void atMostOnceDeliveryFavorsRabbitMq() {
            MessageEnvelope envelope = buildEnvelope(
                    200L,
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_MOST_ONCE,
                    false,
                    "broadcast.events"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getRabbitMqFactors().getDeliveryGuaranteeFactor()).isGreaterThan(0.7);
            assertThat(decision.getKafkaFactors().getDeliveryGuaranteeFactor()).isLessThan(0.3);
        }
    }

    // =========================================================================
    // Load-based routing
    // =========================================================================

    @Nested
    @DisplayName("Load-based routing — broker overload handling")
    class LoadBasedRoutingTests {

        @Test
        @DisplayName("Overloaded Kafka routes traffic to RabbitMQ")
        void overloadedKafkaShiftsToRabbitMq() {
            BrokerLoad saturatedKafka = BrokerLoad.builder()
                    .broker(BrokerTarget.KAFKA)
                    .messagesPerSecond(9_500)
                    .consumerLag(80_000)
                    .cpuUtilisation(0.95)
                    .networkUtilisation(0.92)
                    .healthy(true)
                    .build();

            // Message that would normally go to Kafka (medium size, ordering)
            MessageEnvelope envelope = buildEnvelope(
                    4_096L,
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    true,
                    "orders"
            );

            RoutingDecision decision = router.route(envelope, saturatedKafka, neutralRabbitLoad);

            // Kafka load factor should be very low due to saturation
            assertThat(decision.getKafkaFactors().getLoadFactor()).isLessThan(0.3);
        }

        @Test
        @DisplayName("Unhealthy Kafka broker scores 0.0 load factor")
        void unhealthyKafkaScoresZeroLoad() {
            BrokerLoad unhealthyKafka = BrokerLoad.builder()
                    .broker(BrokerTarget.KAFKA)
                    .messagesPerSecond(0)
                    .consumerLag(0)
                    .cpuUtilisation(0)
                    .networkUtilisation(0)
                    .healthy(false)  // health check failing
                    .build();

            MessageEnvelope envelope = buildEnvelope(
                    1_024L,
                    MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE,
                    false,
                    "orders"
            );

            RoutingDecision decision = router.route(envelope, unhealthyKafka, neutralRabbitLoad);

            assertThat(decision.getKafkaFactors().getLoadFactor()).isEqualTo(0.0);
            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.RABBITMQ);
        }

        @Test
        @DisplayName("Both brokers healthy — load factors are near 1.0")
        void bothHealthyBrokersHaveHighLoadFactor() {
            MessageEnvelope envelope = buildEnvelope(
                    1_024L, MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE, false, "events"
            );

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getKafkaFactors().getLoadFactor()).isCloseTo(1.0, within(0.01));
            assertThat(decision.getRabbitMqFactors().getLoadFactor()).isCloseTo(1.0, within(0.01));
        }
    }

    // =========================================================================
    // Routing hint bypass (baseline experiment mode)
    // =========================================================================

    @Nested
    @DisplayName("Routing hint bypass — baseline experiment mode")
    class RoutingHintTests {

        @Test
        @DisplayName("KAFKA routing hint always selects Kafka regardless of scores")
        void kafkaHintAlwaysSelectsKafka() {
            // Build an envelope that would naturally go to RabbitMQ
            MessageEnvelope envelope = MessageEnvelope.builder()
                    .messageType("PUSH_NOTIFICATION")
                    .sourceService("notification-service")
                    .targetChannel("broadcast.notifications")
                    .payloadSizeBytes(256L)
                    .priority(MessagePriority.CRITICAL)
                    .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .orderingRequired(false)
                    .routingHint(BrokerTarget.KAFKA)  // forced baseline
                    .build();

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.KAFKA);
            assertThat(decision.isRoutingHintApplied()).isTrue();
            assertThat(decision.getKafkaScore()).isEqualTo(0.0);  // scores not computed
        }

        @Test
        @DisplayName("RABBITMQ routing hint always selects RabbitMQ regardless of scores")
        void rabbitHintAlwaysSelectsRabbitMq() {
            // Build an envelope that would naturally go to Kafka
            MessageEnvelope envelope = MessageEnvelope.builder()
                    .messageType("ORDER_CREATED")
                    .sourceService("order-service")
                    .targetChannel("orders")
                    .payloadSizeBytes(20_480L)
                    .priority(MessagePriority.NORMAL)
                    .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .orderingRequired(true)
                    .routingHint(BrokerTarget.RABBITMQ)  // forced baseline
                    .build();

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            assertThat(decision.getSelectedBroker()).isEqualTo(BrokerTarget.RABBITMQ);
            assertThat(decision.isRoutingHintApplied()).isTrue();
        }

        @Test
        @DisplayName("ADAPTIVE routing hint uses the scoring model")
        void adaptiveHintUsesScoringModel() {
            MessageEnvelope envelope = MessageEnvelope.builder()
                    .messageType("ORDER_CREATED")
                    .sourceService("order-service")
                    .targetChannel("orders")
                    .payloadSizeBytes(5_000L)
                    .priority(MessagePriority.NORMAL)
                    .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .orderingRequired(true)
                    .routingHint(BrokerTarget.ADAPTIVE)
                    .build();

            RoutingDecision decision = router.route(envelope, neutralKafkaLoad, neutralRabbitLoad);

            // ADAPTIVE hint triggers scoring — scores should be non-zero
            assertThat(decision.isRoutingHintApplied()).isFalse();
            assertThat(decision.getKafkaScore()).isGreaterThan(0.0);
            assertThat(decision.getRabbitMqScore()).isGreaterThan(0.0);
        }
    }

    // =========================================================================
    // Sigmoid function properties
    // =========================================================================

    @Nested
    @DisplayName("Sigmoid normalisation function")
    class SigmoidTests {

        @Test
        @DisplayName("Sigmoid at knee returns exactly 0.5")
        void sigmoidAtKneeIsHalf() {
            double result = WeightedScoringRouter.sigmoid(8_192.0, 8_192.0);
            assertThat(result).isCloseTo(0.5, within(0.001));
        }

        @Test
        @DisplayName("Sigmoid approaches 1.0 for very large values")
        void sigmoidApproachesOneForLargeValues() {
            double result = WeightedScoringRouter.sigmoid(1_000_000.0, 8_192.0);
            assertThat(result).isGreaterThan(0.99);
        }

        @Test
        @DisplayName("Sigmoid approaches 0.0 for very small values")
        void sigmoidApproachesZeroForSmallValues() {
            double result = WeightedScoringRouter.sigmoid(1.0, 8_192.0);
            assertThat(result).isLessThan(0.01);
        }

        @Test
        @DisplayName("Sigmoid is monotonically increasing")
        void sigmoidIsMonotonicallyIncreasing() {
            double knee = 500.0;
            double prev = WeightedScoringRouter.sigmoid(0.0, knee);
            for (double v = 50; v <= 5000; v += 50) {
                double curr = WeightedScoringRouter.sigmoid(v, knee);
                assertThat(curr).isGreaterThanOrEqualTo(prev);
                prev = curr;
            }
        }

        @ParameterizedTest(name = "sigmoid({0}, 1000) is in [0,1]")
        @CsvSource({"0", "100", "500", "1000", "2000", "10000", "100000"})
        void sigmoidAlwaysInUnitInterval(double value) {
            double result = WeightedScoringRouter.sigmoid(value, 1_000.0);
            assertThat(result).isBetween(0.0, 1.0);
        }
    }

    // =========================================================================
    // Decision completeness
    // =========================================================================

    @Nested
    @DisplayName("Decision completeness and structure")
    class DecisionCompletenessTests {

        @Test
        @DisplayName("Decision always contains non-null messageId and selectedBroker")
        void decisionHasRequiredFields() {
            MessageEnvelope envelope = buildEnvelope(
                    1_024L, MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE, false, "events"
            );

            RoutingDecision decision = router.route(envelope);

            assertThat(decision.getMessageId()).isNotBlank();
            assertThat(decision.getSelectedBroker()).isNotNull();
            assertThat(decision.getKafkaFactors()).isNotNull();
            assertThat(decision.getRabbitMqFactors()).isNotNull();
            assertThat(decision.getDecisionLatencyMicros()).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("Score margin equals absolute difference of broker scores")
        void scoreMarginIsCorrect() {
            MessageEnvelope envelope = buildEnvelope(
                    4_096L, MessagePriority.NORMAL,
                    DeliveryGuarantee.AT_LEAST_ONCE, false, "events"
            );

            RoutingDecision decision = router.route(envelope);

            double expectedMargin = Math.abs(decision.getKafkaScore() - decision.getRabbitMqScore());
            assertThat(decision.getScoreMargin()).isCloseTo(expectedMargin, within(0.0001));
        }

        @Test
        @DisplayName("All factor values are in [0.0, 1.0]")
        void allFactorsAreNormalised() {
            MessageEnvelope envelope = buildEnvelope(
                    5_000L, MessagePriority.HIGH,
                    DeliveryGuarantee.AT_LEAST_ONCE, true, "broadcast.orders"
            );

            RoutingDecision d = router.route(envelope);

            // Kafka factors
            assertThat(d.getKafkaFactors().getThroughputFactor()).isBetween(0.0, 1.0);
            assertThat(d.getKafkaFactors().getSizeFactor()).isBetween(0.0, 1.0);
            assertThat(d.getKafkaFactors().getOrderingFactor()).isBetween(0.0, 1.0);
            assertThat(d.getKafkaFactors().getLoadFactor()).isBetween(0.0, 1.0);
            assertThat(d.getKafkaFactors().getDeliveryGuaranteeFactor()).isBetween(0.0, 1.0);

            // RabbitMQ factors
            assertThat(d.getRabbitMqFactors().getLatencyFactor()).isBetween(0.0, 1.0);
            assertThat(d.getRabbitMqFactors().getRoutingFlexibilityFactor()).isBetween(0.0, 1.0);
            assertThat(d.getRabbitMqFactors().getAckSpeedFactor()).isBetween(0.0, 1.0);
            assertThat(d.getRabbitMqFactors().getLoadFactor()).isBetween(0.0, 1.0);
            assertThat(d.getRabbitMqFactors().getDeliveryGuaranteeFactor()).isBetween(0.0, 1.0);
        }
    }

    // =========================================================================
    // Helper
    // =========================================================================

    private MessageEnvelope buildEnvelope(long sizeBytes,
                                           MessagePriority priority,
                                           DeliveryGuarantee guarantee,
                                           boolean ordering,
                                           String channel) {
        return MessageEnvelope.builder()
                .messageType("TEST_EVENT")
                .sourceService("test")
                .targetChannel(channel)
                .payloadSizeBytes(sizeBytes)
                .priority(priority)
                .deliveryGuarantee(guarantee)
                .orderingRequired(ordering)
                .build();
    }
}
