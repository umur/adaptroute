package com.umurinan.adaptroute.analytics.service;

import com.umurinan.adaptroute.analytics.producer.AnalyticsEventProducer;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Scheduled service that continuously emits analytics events at a configurable rate.
 *
 * <p>Default interval is 20ms (50 msg/s) — the highest base rate of all services.
 * Combined with the large payload size, this produces the most aggressive Kafka
 * affinity activation in the experiment workloads.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyticsEmitterService {

    private final AnalyticsEventProducer producer;

    @Value("${analytics.routing.hint:ADAPTIVE}")
    private String routingHintConfig;

    private final AtomicLong emittedCount = new AtomicLong(0);
    private volatile boolean emitting = true;

    @Scheduled(fixedDelayString = "${analytics.emit.interval-ms:20}")
    public void emitScheduled() {
        if (!emitting) return;

        BrokerTarget hint  = parseHint(routingHintConfig);
        String       msgId = producer.emitAnalyticsEvent(hint);
        if (msgId != null) {
            long count = emittedCount.incrementAndGet();
            if (count % 1000 == 0) {
                log.info("analytics-service: emitted {} events total", count);
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
