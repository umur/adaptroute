package com.umurinan.adaptroute.gateway.consumer;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes messages from all adaptive RabbitMQ queues.
 *
 * <p>Bound to the {@code adaptive.queue} queue, which receives messages from the
 * {@code adaptive.exchange} topic exchange. Measures end-to-end latency from
 * envelope creation to consumer invocation — this reflects RabbitMQ's push-delivery
 * latency, which is the primary metric differentiating it from Kafka in the paper.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RabbitMqMessageConsumer {

    private final ObjectMapper objectMapper;

    private final AtomicLong consumedCount      = new AtomicLong(0);
    private final AtomicLong totalLatencyMicros = new AtomicLong(0);

    /**
     * Listens on the {@code adaptive.queue} bound to {@code adaptive.exchange}.
     * The queue is declared in {@link com.umurinan.adaptroute.gateway.metrics.GatewayRabbitMqConfig}.
     */
    @RabbitListener(queues = "adaptive.queue", ackMode = "AUTO")
    public void onMessage(Message message) {
        try {
            String body = new String(message.getBody());
            MessageEnvelope envelope = objectMapper.readValue(body, MessageEnvelope.class);

            long latencyMicros = computeLatencyMicros(envelope.getCreatedAt());
            consumedCount.incrementAndGet();
            totalLatencyMicros.addAndGet(latencyMicros);

            log.debug("rabbit-consumed: msg={} type={} routing-key={} latency={}µs",
                    envelope.getMessageId(),
                    envelope.getMessageType(),
                    message.getMessageProperties().getReceivedRoutingKey(),
                    latencyMicros);

        } catch (Exception ex) {
            log.error("Failed to deserialise RabbitMQ message: {}", ex.getMessage());
        }
    }

    /** Returns total messages consumed from RabbitMQ since service start. */
    public long getConsumedCount() { return consumedCount.get(); }

    /** Returns average end-to-end latency in microseconds (0 if no messages consumed). */
    public long getAverageLatencyMicros() {
        long count = consumedCount.get();
        return count == 0 ? 0 : totalLatencyMicros.get() / count;
    }

    /** Resets counters — called between experiment runs. */
    public void resetCounters() {
        consumedCount.set(0);
        totalLatencyMicros.set(0);
    }

    private static long computeLatencyMicros(Instant createdAt) {
        return (System.currentTimeMillis() - createdAt.toEpochMilli()) * 1_000;
    }
}
