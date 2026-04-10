package com.umurinan.adaptroute.experiments.workload;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.DeliveryGuarantee;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.MessagePriority;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Generates and dispatches workload messages according to a {@link WorkloadProfile}.
 *
 * <p>The generator is self-contained: it produces realistic synthetic payloads for
 * all three message types (order, notification, analytics) without depending on the
 * producer classes in the service modules. This keeps the experiments module's
 * dependency graph clean — it only needs {@code common}.</p>
 *
 * <p>The generator is invoked three times per profile by the experiment runner:</p>
 * <ol>
 *   <li>With {@code routingHint = KAFKA}    — Kafka-only baseline.</li>
 *   <li>With {@code routingHint = RABBITMQ} — RabbitMQ-only baseline.</li>
 *   <li>With {@code routingHint = ADAPTIVE} — adaptive router (hint = null).</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WorkloadGenerator {

    private final ObjectMapper  objectMapper;
    private final RestTemplate  restTemplate;
    private final Random        random = new Random(42);
    private final Random        jitter = new Random(137);

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Executes a workload run and returns per-message dispatch records.
     *
     * @param profile     the workload characteristics to simulate
     * @param routingHint forces a specific broker (baseline) or ADAPTIVE for scoring model
     * @param gatewayUrl  HTTP URL of the gateway service
     * @return list of dispatch records for CSV export
     */
    public List<DispatchRecord> run(WorkloadProfile profile,
                                    BrokerTarget routingHint,
                                    String gatewayUrl) {

        if (profile.isBurstEnabled()) {
            return runBurst(profile, routingHint, gatewayUrl);
        }

        List<DispatchRecord> records = new ArrayList<>(profile.getTotalMessages());
        int  total     = profile.getTotalMessages();
        int  msgPerSec = profile.getTargetMsgPerSecond();
        long delayMicros = msgPerSec > 0 ? 1_000_000L / msgPerSec : 0;

        log.info("workload-start: profile={} hint={} total={} rate={} msg/s",
                profile.getName(), routingHint, total, msgPerSec);

        long runStartMs = System.currentTimeMillis();

        for (int i = 0; i < total; i++) {
            long dispatchStart = System.nanoTime();

            MessageEnvelope envelope = buildEnvelope(profile, routingHint, i);
            DispatchRecord   record  = dispatch(envelope, gatewayUrl,
                    profile.getName(), routingHint.name(), dispatchStart);
            records.add(record);

            if (delayMicros > 0) {
                long elapsedMicros = (System.nanoTime() - dispatchStart) / 1_000;
                long jitterMicros  = (long) ((jitter.nextDouble() - 0.5) * 0.10 * delayMicros);
                long sleepMicros   = delayMicros - elapsedMicros + jitterMicros;
                if (sleepMicros > 500) {
                    try {
                        Thread.sleep(sleepMicros / 1_000,
                                (int) ((sleepMicros % 1_000) * 1_000));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if ((i + 1) % 5_000 == 0) {
                long   elapsed    = System.currentTimeMillis() - runStartMs;
                double actualRate = (i + 1) * 1000.0 / Math.max(1, elapsed);
                log.info("workload-progress: profile={} {}/{} sent, actual-rate={:.0f} msg/s",
                        profile.getName(), i + 1, total, actualRate);
            }
        }

        long   durationMs  = System.currentTimeMillis() - runStartMs;
        double actualRate  = total * 1000.0 / Math.max(1, durationMs);
        log.info("workload-complete: profile={} hint={} sent={} duration={}ms rate={:.0f} msg/s",
                profile.getName(), routingHint, total, durationMs, actualRate);

        return records;
    }

    /**
     * Time-bounded burst workload: alternates between burst and idle emission phases
     * for the profile's configured duration. Used for W3 (MIXED_WORKLOAD).
     */
    private List<DispatchRecord> runBurst(WorkloadProfile profile,
                                           BrokerTarget routingHint,
                                           String gatewayUrl) {

        List<DispatchRecord> records = new ArrayList<>(5_000);
        long runDeadlineMs = System.currentTimeMillis() + profile.getDurationSeconds() * 1_000L;
        int  msgIndex      = 0;

        // Start in idle phase; first burst follows immediately after first idle.
        boolean inBurst   = false;
        long    phaseEndMs = System.currentTimeMillis()
                + profile.getIdleDurationSeconds() * 1_000L;

        log.info("workload-start burst: profile={} hint={} duration={}s burst={}msg/s idle={}msg/s",
                profile.getName(), routingHint, profile.getDurationSeconds(),
                profile.getBurstMsgPerSecond(), profile.getIdleMsgPerSecond());

        while (System.currentTimeMillis() < runDeadlineMs) {

            // Check if we need to switch phase
            if (System.currentTimeMillis() >= phaseEndMs) {
                inBurst = !inBurst;
                long phaseDurationMs;
                if (inBurst) {
                    // Random burst duration in [burstMin, burstMax]
                    int rangeS = profile.getBurstMaxDurationSeconds()
                            - profile.getBurstMinDurationSeconds();
                    int durationS = profile.getBurstMinDurationSeconds()
                            + (rangeS > 0 ? jitter.nextInt(rangeS + 1) : 0);
                    phaseDurationMs = durationS * 1_000L;
                } else {
                    phaseDurationMs = profile.getIdleDurationSeconds() * 1_000L;
                }
                phaseEndMs = System.currentTimeMillis() + phaseDurationMs;
                log.info("workload-phase-switch: profile={} phase={} duration={}ms",
                        profile.getName(), inBurst ? "BURST" : "IDLE", phaseDurationMs);
            }

            int currentRate = inBurst ? profile.getBurstMsgPerSecond()
                                       : profile.getIdleMsgPerSecond();
            long delayMicros = currentRate > 0 ? 1_000_000L / currentRate : 0;

            long dispatchStart = System.nanoTime();
            MessageEnvelope envelope = buildEnvelope(profile, routingHint, msgIndex++);
            DispatchRecord   record  = dispatch(envelope, gatewayUrl,
                    profile.getName(), routingHint.name(), dispatchStart);
            records.add(record);

            if (delayMicros > 0) {
                long elapsedMicros = (System.nanoTime() - dispatchStart) / 1_000;
                long jitterMicros  = (long) ((jitter.nextDouble() - 0.5) * 0.10 * delayMicros);
                long sleepMicros   = delayMicros - elapsedMicros + jitterMicros;
                if (sleepMicros > 500) {
                    try {
                        Thread.sleep(sleepMicros / 1_000,
                                (int) ((sleepMicros % 1_000) * 1_000));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if (records.size() % 10_000 == 0) {
                long elapsed = System.currentTimeMillis()
                        - (runDeadlineMs - profile.getDurationSeconds() * 1_000L);
                double actualRate = records.size() * 1000.0 / Math.max(1, elapsed);
                log.info("workload-progress burst: profile={} sent={} phase={} actual-rate={:.0f} msg/s",
                        profile.getName(), records.size(),
                        inBurst ? "BURST" : "IDLE", actualRate);
            }
        }

        long runStartMs = runDeadlineMs - profile.getDurationSeconds() * 1_000L;
        long durationMs = System.currentTimeMillis() - runStartMs;
        double actualRate = records.size() * 1000.0 / Math.max(1, durationMs);
        log.info("workload-complete burst: profile={} hint={} sent={} duration={}ms rate={:.0f} msg/s",
                profile.getName(), routingHint, records.size(), durationMs, actualRate);

        return records;
    }

    // =========================================================================
    // Envelope construction — message type mix driven by profile fractions
    // =========================================================================

    private MessageEnvelope buildEnvelope(WorkloadProfile profile,
                                           BrokerTarget routingHint,
                                           int index) {
        double r = random.nextDouble();
        BrokerTarget hint = (routingHint == BrokerTarget.ADAPTIVE) ? null : routingHint;

        if (r < profile.getOrderFraction()) {
            return buildOrderEnvelope(profile, hint);
        } else if (r < profile.getOrderFraction() + profile.getNotificationFraction()) {
            return buildNotificationEnvelope(profile, hint);
        } else {
            return buildAnalyticsEnvelope(hint);
        }
    }

    // -------------------------------------------------------------------------
    // Order event envelope — high throughput, ordering critical, EXACTLY_ONCE
    // -------------------------------------------------------------------------

    private MessageEnvelope buildOrderEnvelope(WorkloadProfile profile, BrokerTarget hint) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId",       "ORD-" + shortUuid());
        payload.put("customerId",    "CUST-" + (10000 + random.nextInt(90000)));
        payload.put("totalAmount",   round(9.99 + random.nextDouble() * 990));
        payload.put("currency",      "USD");
        payload.put("status",        "PENDING");
        payload.put("paymentMethod", random.nextBoolean() ? "CREDIT_CARD" : "PAYPAL");
        payload.put("occurredAt",    Instant.now().toString());
        payload.put("eventType",     ORDER_TYPES[random.nextInt(ORDER_TYPES.length)]);

        // Add line items to reach realistic 2–15 KB payload
        List<Map<String, Object>> items = new ArrayList<>();
        int itemCount = 1 + random.nextInt(6);
        for (int i = 0; i < itemCount; i++) {
            Map<String, Object> item = new HashMap<>();
            item.put("productId",   "PROD-" + random.nextInt(10000));
            item.put("productName", PRODUCTS[random.nextInt(PRODUCTS.length)]);
            item.put("quantity",    1 + random.nextInt(4));
            item.put("unitPrice",   round(9.99 + random.nextDouble() * 490));
            item.put("category",    CATEGORIES[random.nextInt(CATEGORIES.length)]);
            items.add(item);
        }
        payload.put("lineItems", items);
        payload.put("shippingAddress", (100 + random.nextInt(9900))
                + " Main St, " + COUNTRIES[random.nextInt(COUNTRIES.length)]);

        return toEnvelope("ORDER_CREATED", "order-service", "orders",
                payload, MessagePriority.NORMAL, DeliveryGuarantee.EXACTLY_ONCE,
                profile.isOrderingCritical(), hint);
    }

    // -------------------------------------------------------------------------
    // Notification event envelope — low latency, fanout, AT_LEAST_ONCE
    // -------------------------------------------------------------------------

    private MessageEnvelope buildNotificationEnvelope(WorkloadProfile profile,
                                                       BrokerTarget hint) {
        boolean isHigh = random.nextDouble() < profile.getHighPriorityFraction();
        MessagePriority priority = isHigh ? MessagePriority.HIGH : MessagePriority.NORMAL;

        String[] tmpl = EMAIL_TEMPLATES[random.nextInt(EMAIL_TEMPLATES.length)];
        Map<String, Object> payload = new HashMap<>();
        payload.put("notificationId",  UUID.randomUUID().toString());
        payload.put("type",            NOTIF_TYPES[random.nextInt(NOTIF_TYPES.length)]);
        payload.put("recipientId",     "USER-" + (10000 + random.nextInt(90000)));
        payload.put("subject",         tmpl[1]);
        payload.put("body",            "Dear customer, " + tmpl[1] + ". Please review.");
        payload.put("templateId",      tmpl[0]);
        payload.put("priority",        priority.name());
        payload.put("broadcastToAll",  random.nextDouble() < 0.3);
        payload.put("occurredAt",      Instant.now().toString());

        return toEnvelope(NOTIF_TYPES[random.nextInt(NOTIF_TYPES.length)],
                "notification-service", "broadcast.notifications",
                payload, priority, DeliveryGuarantee.AT_LEAST_ONCE, false, hint);
    }

    // -------------------------------------------------------------------------
    // Analytics event envelope — high throughput, large payload, AT_LEAST_ONCE
    // -------------------------------------------------------------------------

    private MessageEnvelope buildAnalyticsEnvelope(BrokerTarget hint) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("eventId",       UUID.randomUUID().toString());
        payload.put("eventType",     ANALYTICS_TYPES[random.nextInt(ANALYTICS_TYPES.length)]);
        payload.put("sessionId",     "sess-" + shortUuid());
        payload.put("userId",        random.nextBoolean() ? "USER-" + random.nextInt(90000) : null);
        payload.put("userAgent",     "Mozilla/5.0 (" + OS_LIST[random.nextInt(OS_LIST.length)] + ")");
        payload.put("ipAddress",     randomIp());
        payload.put("deviceType",    DEVICE_TYPES[random.nextInt(DEVICE_TYPES.length)]);
        payload.put("country",       COUNTRIES[random.nextInt(COUNTRIES.length)]);
        payload.put("pageUrl",       "https://shop.example.com/page-" + random.nextInt(500));
        payload.put("occurredAt",    Instant.now().toString());
        payload.put("viewportWidth", 1024 + random.nextInt(896));
        payload.put("loadTimeMs",    200 + random.nextInt(3000));

        // Inflate payload to 5–50 KB with experiment assignments and properties
        Map<String, String> experiments = new HashMap<>();
        for (int i = 0; i < 8 + random.nextInt(12); i++) {
            experiments.put("exp-" + (100 + i), random.nextBoolean() ? "control" : "variant-a");
        }
        payload.put("experimentAssignments", experiments);

        // Custom property bag
        Map<String, Object> props = new HashMap<>();
        for (int i = 0; i < 10 + random.nextInt(20); i++) {
            props.put("prop_" + i, "value_" + random.nextInt(10000));
        }
        payload.put("properties", props);

        // Funnel steps array to further inflate size
        List<String> funnel = new ArrayList<>();
        for (int i = 0; i < 3 + random.nextInt(8); i++) {
            funnel.add("https://shop.example.com/step-" + i + "/" + shortUuid());
        }
        payload.put("funnelSteps", funnel);

        return toEnvelope(ANALYTICS_TYPES[random.nextInt(ANALYTICS_TYPES.length)],
                "analytics-service", "analytics.events",
                payload, MessagePriority.LOW, DeliveryGuarantee.AT_LEAST_ONCE, false, hint);
    }

    // =========================================================================
    // Envelope builder helper
    // =========================================================================

    private MessageEnvelope toEnvelope(String messageType,
                                        String sourceService,
                                        String targetChannel,
                                        Map<String, Object> payloadMap,
                                        MessagePriority priority,
                                        DeliveryGuarantee guarantee,
                                        boolean ordering,
                                        BrokerTarget hint) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(payloadMap);
            return MessageEnvelope.builder()
                    .messageType(messageType)
                    .sourceService(sourceService)
                    .targetChannel(targetChannel)
                    .payload(objectMapper.valueToTree(payloadMap))
                    .payloadSizeBytes(bytes.length)
                    .priority(priority)
                    .deliveryGuarantee(guarantee)
                    .orderingRequired(ordering)
                    .routingHint(hint)
                    .build();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to build envelope for " + messageType, ex);
        }
    }

    // =========================================================================
    // Dispatch and record
    // =========================================================================

    private DispatchRecord dispatch(MessageEnvelope envelope,
                                     String gatewayUrl,
                                     String profileName,
                                     String routingMode,
                                     long startNanos) {
        String  selectedBroker         = "ERROR";
        double  kafkaScore             = 0.0;
        double  rabbitScore            = 0.0;
        long    decisionLatencyMicros  = 0L;
        boolean success                = false;
        String  errorType              = null;

        try {
            @SuppressWarnings("unchecked")
            ResponseEntity<Map> response =
                    restTemplate.postForEntity(gatewayUrl + "/api/route", envelope, Map.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<?, ?> body = response.getBody();
                Object brokerVal = body.get("selectedBroker");
                selectedBroker        = brokerVal != null ? String.valueOf(brokerVal) : "UNKNOWN";
                kafkaScore            = toDouble(body.get("kafkaScore"));
                rabbitScore           = toDouble(body.get("rabbitMqScore"));
                decisionLatencyMicros = toLong(body.get("decisionLatencyMicros"));
                success               = true;
            }
        } catch (Exception ex) {
            errorType = ex.getClass().getSimpleName();
            log.warn("dispatch failed for msg={}: {}", envelope.getMessageId(), ex.getMessage());
        }

        long latencyMicros = (System.nanoTime() - startNanos) / 1_000;

        return new DispatchRecord(
                envelope.getMessageId(),
                envelope.getMessageType(),
                envelope.getSourceService(),
                profileName,
                routingMode,
                selectedBroker,
                kafkaScore,
                rabbitScore,
                Math.abs(kafkaScore - rabbitScore),
                envelope.getPayloadSizeBytes(),
                envelope.isOrderingRequired(),
                envelope.getDeliveryGuarantee().name(),
                envelope.getPriority().name(),
                latencyMicros,
                decisionLatencyMicros,
                success,
                errorType
        );
    }

    // =========================================================================
    // Utilities
    // =========================================================================

    private static double toDouble(Object val) {
        if (val == null) return 0.0;
        if (val instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(val.toString()); }
        catch (NumberFormatException e) { return 0.0; }
    }

    private static long toLong(Object val) {
        if (val == null) return 0L;
        if (val instanceof Number n) return n.longValue();
        try { return Long.parseLong(val.toString()); }
        catch (NumberFormatException e) { return 0L; }
    }

    private static double round(double v) {
        return BigDecimal.valueOf(v).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    private static String shortUuid() {
        return UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    private String randomIp() {
        return (1 + random.nextInt(222)) + "."
                + random.nextInt(256) + "."
                + random.nextInt(256) + "."
                + (1 + random.nextInt(254));
    }

    // =========================================================================
    // Static data tables
    // =========================================================================

    private static final String[] ORDER_TYPES = {
            "ORDER_CREATED", "ORDER_PAYMENT_PROCESSED", "ORDER_SHIPPED",
            "ORDER_COMPLETED", "ORDER_CANCELLED"
    };

    private static final String[] PRODUCTS = {
            "Wireless Headphones", "Running Shoes", "Coffee Maker", "Yoga Mat",
            "Mechanical Keyboard", "Desk Lamp", "Water Bottle", "Backpack",
            "Smart Watch", "Standing Desk", "Monitor", "USB Hub", "Webcam"
    };

    private static final String[] CATEGORIES = {
            "Electronics", "Footwear", "Appliances", "Sports", "Office", "Accessories"
    };

    private static final String[] COUNTRIES = {
            "US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "SG"
    };

    private static final String[] NOTIF_TYPES = {
            "EMAIL_NOTIFICATION", "SMS_ALERT", "PUSH_NOTIFICATION", "IN_APP_NOTIFICATION"
    };

    private static final String[][] EMAIL_TEMPLATES = {
            {"TMPL-ORDER-CONFIRM", "Your order has been confirmed"},
            {"TMPL-SHIP-NOTIF",    "Your order has shipped"},
            {"TMPL-PROMO-WEEKLY",  "This week's deals just for you"},
            {"TMPL-ACCOUNT-ALERT", "Unusual activity on your account"},
            {"TMPL-WELCOME",       "Welcome to our platform"}
    };

    private static final String[] ANALYTICS_TYPES = {
            "PAGE_VIEW", "CLICK_EVENT", "CONVERSION", "SESSION_SUMMARY",
            "SEARCH_QUERY", "PRODUCT_VIEW", "ADD_TO_CART", "CHECKOUT_STARTED"
    };

    private static final String[] OS_LIST = {
            "Windows 11", "macOS 14", "Ubuntu 22.04", "iOS 17", "Android 14"
    };

    private static final String[] DEVICE_TYPES = {"desktop", "mobile", "tablet"};

    // =========================================================================
    // Per-message record (written to CSV)
    // =========================================================================

    /**
     * Holds the result of a single message dispatch for CSV export.
     * All fields are plain types to simplify CSV serialisation.
     */
    public record DispatchRecord(
            String  messageId,
            String  messageType,
            String  sourceService,
            String  profileName,
            String  routingMode,         // KAFKA | RABBITMQ | ADAPTIVE
            String  selectedBroker,
            double  kafkaScore,
            double  rabbitMqScore,
            double  scoreMargin,
            long    payloadSizeBytes,
            boolean orderingRequired,
            String  deliveryGuarantee,
            String  priority,
            long    latencyMicros,
            long    decisionLatencyMicros, // scoring decision time from gateway (0 for baselines)
            boolean success,
            String  errorType            // null if success
    ) {}
}
