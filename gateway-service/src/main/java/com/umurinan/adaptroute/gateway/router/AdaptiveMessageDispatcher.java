package com.umurinan.adaptroute.gateway.router;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.BrokerLoad;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.RoutingDecision;
import com.umurinan.adaptroute.common.metrics.BrokerLoadMonitor;
import com.umurinan.adaptroute.common.metrics.RoutingMetricsRegistry;
import com.umurinan.adaptroute.common.router.MessageRouter;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Adaptive message dispatcher — the runtime execution engine for routing decisions.
 *
 * <p>For each incoming {@link MessageEnvelope}:</p>
 * <ol>
 *   <li>Fetches the latest broker load snapshots from {@link BrokerLoadMonitor}.</li>
 *   <li>Calls {@link MessageRouter#route} to obtain a {@link RoutingDecision}.</li>
 *   <li>Records the decision in {@link RoutingMetricsRegistry}.</li>
 *   <li>Dispatches to the selected broker via circuit-breaker-protected methods.</li>
 *   <li>On broker failure, falls back to the alternative broker; if both fail,
 *       enqueues the message in the bounded in-memory failover buffer for
 *       retry once health recovers.</li>
 * </ol>
 *
 * <p>Circuit breakers (Resilience4j) protect both dispatch paths. The annotated
 * methods ({@link #dispatchToKafka} and {@link #dispatchToRabbitMq}) are {@code public}
 * so that Spring AOP can proxy them; calls from within this class use the
 * {@link #self} reference to route through the proxy.</p>
 *
 * <p>When both brokers are unavailable, undeliverable messages are queued in a
 * bounded {@link LinkedBlockingQueue} with capacity {@code adaptroute.buffer.capacity}
 * (default: 10,000). A {@link Scheduled} drain task retries delivery every 500 ms
 * once at least one broker becomes healthy. If the buffer is full, messages are
 * dropped and a {@code buffer_overflow} metric is recorded.</p>
 *
 * <p>Kafka sends are wrapped in producer transactions (via
 * {@link KafkaTemplate#executeInTransaction}) to satisfy EXACTLY_ONCE delivery
 * semantics when the producer factory is configured with a transactional ID prefix.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AdaptiveMessageDispatcher {

    private final MessageRouter messageRouter;
    private final BrokerLoadMonitor loadMonitor;
    private final RoutingMetricsRegistry metricsRegistry;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Self-reference injected lazily so that Spring AOP circuit-breaker proxies
     * are active on {@link #dispatchToKafka} and {@link #dispatchToRabbitMq} calls
     * originating from within this bean. Without this, {@code this.method()} bypasses
     * the proxy and the circuit breaker has no effect.
     */
    @Lazy
    @Autowired
    private AdaptiveMessageDispatcher self;

    /** Bounded failover buffer capacity; configurable via {@code adaptroute.buffer.capacity}. */
    @Value("${adaptroute.buffer.capacity:10000}")
    private int bufferCapacity;

    /**
     * Bounded in-memory failover buffer. Populated when both brokers fail simultaneously.
     * Drained every 500 ms by {@link #drainFailoverBuffer()} once health recovers.
     * Initialized in {@link #init()} after {@code @Value} injection completes.
     */
    private LinkedBlockingQueue<MessageEnvelope> failoverBuffer;

    // RabbitMQ exchange name — messages are published here and fanned out to queues
    private static final String RABBIT_EXCHANGE = "adaptive.exchange";

    @PostConstruct
    void init() {
        failoverBuffer = new LinkedBlockingQueue<>(bufferCapacity);
    }

    // =========================================================================
    // Primary dispatch pipeline
    // =========================================================================

    /**
     * Dispatches a message envelope through the adaptive routing pipeline.
     *
     * @param envelope the message to dispatch
     * @return the routing decision that was applied
     */
    public RoutingDecision dispatch(MessageEnvelope envelope) {
        BrokerLoad kafkaLoad  = loadMonitor.getKafkaLoad();
        BrokerLoad rabbitLoad = loadMonitor.getRabbitMqLoad();

        RoutingDecision decision = messageRouter.route(envelope, kafkaLoad, rabbitLoad);

        metricsRegistry.recordDecision(decision, envelope.getPayloadSizeBytes());

        log.info("dispatch: msg={} type={} broker={} kafka_score={:.3f} rabbit_score={:.3f}",
                envelope.getMessageId(),
                envelope.getMessageType(),
                decision.getSelectedBroker(),
                decision.getKafkaScore(),
                decision.getRabbitMqScore());

        try {
            BrokerTarget target = decision.getSelectedBroker();
            if (target == BrokerTarget.STATIC_SPLIT) {
                target = resolveStaticSplit(envelope);
            }
            if (target == BrokerTarget.KAFKA) {
                self.dispatchToKafka(envelope, decision);
            } else {
                self.dispatchToRabbitMq(envelope, decision);
            }
        } catch (Exception ex) {
            log.error("Primary dispatch failed for msg={}, attempting fallback. Error: {}",
                    envelope.getMessageId(), ex.getMessage());
            applyFallback(envelope, decision, ex);
        }

        return decision;
    }

    // =========================================================================
    // Kafka dispatch
    // =========================================================================

    /**
     * Dispatches a message to Kafka within a producer transaction.
     *
     * <p>This method is {@code public} so that the Spring AOP proxy can intercept it
     * for circuit-breaker enforcement. Always invoke via {@link #self}, never as
     * {@code this.dispatchToKafka(...)}.</p>
     *
     * <p>The message type is used as the Kafka partition key so that all messages of
     * the same class are routed to the same partition, preserving per-class ordering.</p>
     *
     * <p>The send is wrapped in {@link KafkaTemplate#executeInTransaction} to provide
     * exactly-once delivery semantics when the producer factory is configured with
     * {@code transactionIdPrefix} (see {@code GatewayKafkaConfig}).</p>
     */
    @CircuitBreaker(name = "kafka-dispatcher", fallbackMethod = "kafkaFallback")
    public void dispatchToKafka(MessageEnvelope envelope, RoutingDecision decision) {
        try {
            String topic = resolveKafkaTopic(envelope.getTargetChannel());
            // Use messageType (not messageId) as the partition key to guarantee
            // per-class ordering: all messages of the same class go to the same partition.
            String key     = envelope.getMessageType();
            String payload = objectMapper.writeValueAsString(envelope);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.executeInTransaction(ops -> ops.send(topic, key, payload));

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    metricsRegistry.recordError(BrokerTarget.KAFKA, "send_failure");
                    log.error("Kafka send failed for msg={}: {}", envelope.getMessageId(), ex.getMessage());
                } else {
                    loadMonitor.incrementKafkaSent();
                    long latencyMicros = computeLatencyMicros(envelope.getCreatedAt());
                    metricsRegistry.recordDispatch(BrokerTarget.KAFKA, latencyMicros);
                    log.debug("Kafka ack: msg={} partition={} offset={}",
                            envelope.getMessageId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                }
            });

        } catch (Exception ex) {
            metricsRegistry.recordError(BrokerTarget.KAFKA, "serialisation_error");
            throw new RuntimeException("Kafka dispatch error", ex);
        }
    }

    /**
     * Circuit-breaker fallback for {@link #dispatchToKafka}.
     *
     * <p>For ordering-required messages, routes to the failover buffer (block-and-retry)
     * rather than falling back to RabbitMQ.  Sending an ordering-required message to
     * RabbitMQ would violate the per-partition ordering guarantee, so the message is
     * held in the bounded in-memory queue and re-dispatched to Kafka once the health
     * probe reports recovery (see {@link #drainFailoverBuffer}).
     *
     * <p>For non-ordering messages, falls back to RabbitMQ as normal.
     */
    public void kafkaFallback(MessageEnvelope envelope, RoutingDecision decision, Throwable cause) {
        if (envelope.isOrderingRequired()) {
            log.warn("Kafka unavailable for ordering-required msg={}; buffering for block-and-retry. Cause: {}",
                    envelope.getMessageId(), cause.getMessage());
            enqueueFailoverBuffer(envelope);
            return;
        }
        log.warn("Kafka circuit breaker open for msg={}; applying fallback. Cause: {}",
                envelope.getMessageId(), cause.getMessage());
        applyFallback(envelope, decision, new RuntimeException(cause));
    }

    // =========================================================================
    // RabbitMQ dispatch
    // =========================================================================

    /**
     * Dispatches a message to RabbitMQ.
     *
     * <p>This method is {@code public} so that the Spring AOP proxy can intercept it
     * for circuit-breaker enforcement. Always invoke via {@link #self}.</p>
     */
    @CircuitBreaker(name = "rabbitmq-dispatcher", fallbackMethod = "rabbitFallback")
    public void dispatchToRabbitMq(MessageEnvelope envelope, RoutingDecision decision) {
        try {
            String routingKey = resolveRabbitRoutingKey(envelope.getTargetChannel(),
                    envelope.getMessageType());
            String payload    = objectMapper.writeValueAsString(envelope);

            rabbitTemplate.convertAndSend(RABBIT_EXCHANGE, routingKey, payload);

            loadMonitor.incrementRabbitSent();
            long latencyMicros = computeLatencyMicros(envelope.getCreatedAt());
            metricsRegistry.recordDispatch(BrokerTarget.RABBITMQ, latencyMicros);

        } catch (Exception ex) {
            metricsRegistry.recordError(BrokerTarget.RABBITMQ, "send_failure");
            throw new RuntimeException("RabbitMQ dispatch error", ex);
        }
    }

    /**
     * Circuit-breaker fallback for {@link #dispatchToRabbitMq}.
     */
    public void rabbitFallback(MessageEnvelope envelope, RoutingDecision decision, Throwable cause) {
        log.warn("RabbitMQ circuit breaker open for msg={}; applying fallback. Cause: {}",
                envelope.getMessageId(), cause.getMessage());
        applyFallback(envelope, decision, new RuntimeException(cause));
    }

    // =========================================================================
    // Fallback / graceful degradation
    // =========================================================================

    /**
     * Falls back to the alternative broker when the primary broker is unavailable.
     * If the fallback also fails, the message is enqueued in the bounded failover
     * buffer via {@link #enqueueFailoverBuffer}.
     */
    private void applyFallback(MessageEnvelope envelope, RoutingDecision decision, Exception cause) {
        BrokerTarget primary  = decision.getSelectedBroker();
        BrokerTarget fallback = (primary == BrokerTarget.KAFKA)
                ? BrokerTarget.RABBITMQ
                : BrokerTarget.KAFKA;

        metricsRegistry.recordFallback(primary, fallback);

        log.warn("Applying fallback: {} -> {} for msg={} cause={}",
                primary, fallback, envelope.getMessageId(), cause.getMessage());

        try {
            if (fallback == BrokerTarget.KAFKA) {
                self.dispatchToKafka(envelope, decision);
            } else {
                self.dispatchToRabbitMq(envelope, decision);
            }
        } catch (Exception fallbackEx) {
            metricsRegistry.recordError(fallback, "fallback_failure");
            log.error("Fallback dispatch also failed for msg={}: {}",
                    envelope.getMessageId(), fallbackEx.getMessage());
            enqueueFailoverBuffer(envelope);
        }
    }

    /**
     * Enqueues a message in the in-memory failover buffer.
     * Logs a warning on success. Records {@code buffer_overflow} and drops the
     * message if the buffer has reached its configured capacity.
     */
    private void enqueueFailoverBuffer(MessageEnvelope envelope) {
        if (failoverBuffer.offer(envelope)) {
            log.warn("Both brokers unavailable; buffered msg={} (buffer size={})",
                    envelope.getMessageId(), failoverBuffer.size());
        } else {
            metricsRegistry.recordError(BrokerTarget.KAFKA, "buffer_overflow");
            log.error("Failover buffer full (capacity={}); dropping msg={}",
                    bufferCapacity, envelope.getMessageId());
        }
    }

    /**
     * Drains the in-memory failover buffer whenever at least one broker is healthy.
     * Runs every 500 ms in the Spring scheduler thread pool. At most 100 messages
     * are dispatched per invocation to limit latency spikes. Backs off immediately
     * if dispatch fails again, retrying on the next scheduled invocation.
     */
    @Scheduled(fixedDelay = 500)
    public void drainFailoverBuffer() {
        if (failoverBuffer.isEmpty()) return;

        boolean kafkaUp  = loadMonitor.isKafkaHealthy();
        boolean rabbitUp = loadMonitor.isRabbitHealthy();
        if (!kafkaUp && !rabbitUp) return;

        int maxPerCycle = 100;
        int drained = 0;
        MessageEnvelope envelope;

        while (drained < maxPerCycle && (envelope = failoverBuffer.poll()) != null) {
            try {
                BrokerLoad kafkaLoad  = loadMonitor.getKafkaLoad();
                BrokerLoad rabbitLoad = loadMonitor.getRabbitMqLoad();
                RoutingDecision decision = messageRouter.route(envelope, kafkaLoad, rabbitLoad);
                BrokerTarget target = decision.getSelectedBroker();
                if (target == BrokerTarget.STATIC_SPLIT) {
                    target = resolveStaticSplit(envelope);
                }
                if (target == BrokerTarget.KAFKA && kafkaUp) {
                    self.dispatchToKafka(envelope, decision);
                } else {
                    self.dispatchToRabbitMq(envelope, decision);
                }
                drained++;
            } catch (Exception ex) {
                log.warn("Buffer drain failed for msg={}; re-queuing", envelope.getMessageId());
                if (!failoverBuffer.offer(envelope)) {
                    metricsRegistry.recordError(BrokerTarget.KAFKA, "buffer_overflow");
                    log.error("Failover buffer full during drain; dropping msg={}", envelope.getMessageId());
                }
                break;  // Back off; retry on next scheduled invocation
            }
        }

        if (drained > 0) {
            log.info("Buffer drain: dispatched {} messages, remaining={}", drained, failoverBuffer.size());
        }
    }

    // =========================================================================
    // Static Split resolution
    // =========================================================================

    /**
     * Resolves the physical broker target for STATIC_SPLIT routing.
     * Rules:
     *   - ORDER messages      → Kafka  (high throughput, ordering required)
     *   - NOTIFICATION msgs   → RabbitMQ (low latency, push delivery)
     *   - ANALYTICS messages  → hash-based 50/50 split
     */
    private static BrokerTarget resolveStaticSplit(MessageEnvelope envelope) {
        String type = envelope.getMessageType();
        if (type != null) {
            String upper = type.toUpperCase();
            if (upper.contains("ORDER")) {
                return BrokerTarget.KAFKA;
            }
            if (upper.contains("NOTIFICATION") || upper.contains("ALERT")
                    || upper.contains("EMAIL") || upper.contains("SMS")
                    || upper.contains("PUSH") || upper.contains("IN_APP")) {
                return BrokerTarget.RABBITMQ;
            }
        }
        // ANALYTICS and unknown types: hash-based 50/50 split
        int hash = envelope.getMessageId() != null ? envelope.getMessageId().hashCode() : 0;
        return (hash % 2 == 0) ? BrokerTarget.KAFKA : BrokerTarget.RABBITMQ;
    }

    // =========================================================================
    // Channel resolution helpers
    // =========================================================================

    /**
     * Maps a logical channel name to a Kafka topic name.
     * Convention: replace dots with hyphens and prefix with "adaptive-".
     */
    private static String resolveKafkaTopic(String targetChannel) {
        if (targetChannel == null) return "adaptive-default";
        return "adaptive-" + targetChannel.toLowerCase().replace(".", "-");
    }

    /**
     * Maps a logical channel and message type to a RabbitMQ routing key.
     * Convention: {@code <channel>.<messageType>} in dot notation.
     */
    private static String resolveRabbitRoutingKey(String targetChannel, String messageType) {
        String channel = (targetChannel != null) ? targetChannel.toLowerCase() : "default";
        String type    = (messageType   != null) ? messageType.toLowerCase()   : "unknown";
        return channel + "." + type;
    }

    /**
     * Computes the elapsed time in microseconds since the envelope was created.
     * Uses {@link Instant#now()} for both endpoints to avoid mixed-clock arithmetic.
     */
    private static long computeLatencyMicros(Instant createdAt) {
        return Math.max(0, Duration.between(createdAt, Instant.now()).toNanos() / 1_000);
    }
}
