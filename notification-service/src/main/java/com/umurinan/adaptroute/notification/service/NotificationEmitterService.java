package com.umurinan.adaptroute.notification.service;

import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.notification.producer.NotificationEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Scheduled service that continuously emits notification events at a configurable rate.
 *
 * <p>Default interval is 50ms (20 msg/s), producing a higher-frequency but smaller
 * message stream than the order service. This simulates real-world notification
 * systems where many small, latency-sensitive messages are dispatched concurrently.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationEmitterService {

    private final NotificationEventProducer producer;

    @Value("${notification.routing.hint:ADAPTIVE}")
    private String routingHintConfig;

    private final AtomicLong emittedCount = new AtomicLong(0);
    private volatile boolean emitting = true;

    @Scheduled(fixedDelayString = "${notification.emit.interval-ms:50}")
    public void emitScheduled() {
        if (!emitting) return;

        BrokerTarget hint  = parseHint(routingHintConfig);
        String       msgId = producer.emitNotificationEvent(hint);
        if (msgId != null) {
            long count = emittedCount.incrementAndGet();
            if (count % 1000 == 0) {
                log.info("notification-service: emitted {} events total", count);
            }
        }
    }

    public void pause()  { emitting = false; }
    public void resume() { emitting = true; }
    public long getEmittedCount() { return emittedCount.get(); }
    public void resetCounter()    { emittedCount.set(0); }

    private static BrokerTarget parseHint(String hint) {
        try {
            return BrokerTarget.valueOf(hint.toUpperCase());
        } catch (IllegalArgumentException e) {
            return BrokerTarget.ADAPTIVE;
        }
    }
}
