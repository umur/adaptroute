package com.umurinan.adaptroute.common.router;

import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.RoutingDecision;

/**
 * Core abstraction for the adaptive message routing algorithm.
 *
 * <p>Implementations accept a {@link MessageEnvelope} together with the current
 * {@link BrokerLoad} snapshots for both brokers and return a fully documented
 * {@link RoutingDecision} that the dispatcher uses to select a physical broker.</p>
 *
 * <p>The primary implementation, {@link WeightedScoringRouter}, uses the weighted
 * scoring model described in the research paper:</p>
 *
 * <pre>
 *   score_kafka  = w1·throughputFactor + w2·sizeFactor
 *                + w3·orderingFactor   + w4·loadFactor_kafka
 *                + w5·deliveryGuaranteeFactor
 *
 *   score_rabbit = w1·latencyFactor + w2·routingFlexibilityFactor
 *                + w3·ackSpeedFactor + w4·loadFactor_rabbit
 *                + w5·deliveryGuaranteeFactor
 *
 *   decision = argmax(score_kafka, score_rabbit)
 * </pre>
 *
 * <p>Alternative implementations (e.g., ML-based, rule-based) can be swapped in
 * via Spring's {@code @Primary} mechanism without changing any client code.</p>
 */
public interface MessageRouter {

    /**
     * Determines the optimal broker for the given message.
     *
     * @param envelope     the message to be routed; must not be {@code null}
     * @param kafkaLoad    current load snapshot for the Kafka cluster
     * @param rabbitMqLoad current load snapshot for the RabbitMQ broker
     * @return a {@link RoutingDecision} containing the selected broker and full
     *         scoring breakdown for observability and experiment analysis
     */
    RoutingDecision route(MessageEnvelope envelope,
                          BrokerLoad kafkaLoad,
                          BrokerLoad rabbitMqLoad);

    /**
     * Convenience overload that uses neutral (zero) load snapshots.
     * Useful in unit tests and scenarios where broker-load monitoring is disabled.
     *
     * @param envelope the message to be routed
     * @return routing decision based solely on message characteristics
     */
    default RoutingDecision route(MessageEnvelope envelope) {
        return route(
                envelope,
                BrokerLoad.neutral(MessageEnvelope.BrokerTarget.KAFKA),
                BrokerLoad.neutral(MessageEnvelope.BrokerTarget.RABBITMQ)
        );
    }
}
