package com.umurinan.adaptroute.order.service;

import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.order.producer.OrderEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Scheduled service that continuously emits order events at a configurable rate.
 *
 * <p>The emit rate is controlled by {@code order.emit.interval-ms} (default 100ms = 10 msg/s).
 * The experiment runner overrides this via REST or by adjusting the interval property
 * to simulate burst and steady-state workload profiles.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEmitterService {

    private final OrderEventProducer producer;

    @Value("${order.routing.hint:ADAPTIVE}")
    private String routingHintConfig;

    private final AtomicLong emittedCount = new AtomicLong(0);
    private volatile boolean emitting = true;

    /**
     * Emits one order event per scheduled interval.
     * The fixed-delay scheduler ensures backpressure: if the gateway is slow,
     * the emitter waits rather than queuing unbounded work.
     */
    @Scheduled(fixedDelayString = "${order.emit.interval-ms:100}")
    public void emitScheduled() {
        if (!emitting) return;

        BrokerTarget hint = parseHint(routingHintConfig);
        String msgId = producer.emitOrderEvent(hint);
        if (msgId != null) {
            long count = emittedCount.incrementAndGet();
            if (count % 1000 == 0) {
                log.info("order-service: emitted {} events total", count);
            }
        }
    }

    /** Pauses scheduled emission (used by experiment runner between runs). */
    public void pause()  { emitting = false; }

    /** Resumes scheduled emission. */
    public void resume() { emitting = true; }

    /** Returns total events emitted since service start. */
    public long getEmittedCount() { return emittedCount.get(); }

    /** Resets the counter — called at the start of each experiment run. */
    public void resetCounter() { emittedCount.set(0); }

    private static BrokerTarget parseHint(String hint) {
        try {
            return BrokerTarget.valueOf(hint.toUpperCase());
        } catch (IllegalArgumentException e) {
            return BrokerTarget.ADAPTIVE;
        }
    }
}
