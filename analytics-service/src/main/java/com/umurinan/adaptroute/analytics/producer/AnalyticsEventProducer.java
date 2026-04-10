package com.umurinan.adaptroute.analytics.producer;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.analytics.model.AnalyticsEvent;
import com.umurinan.adaptroute.analytics.model.AnalyticsEvent.EventType;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.DeliveryGuarantee;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.MessagePriority;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Produces rich analytics events and submits them to the gateway router.
 *
 * <p>Each emitted {@link MessageEnvelope} has:</p>
 * <ul>
 *   <li>{@code orderingRequired = false} — analytics events are independently processable.</li>
 *   <li>{@code deliveryGuarantee = AT_LEAST_ONCE} — duplicates are acceptable for analytics.</li>
 *   <li>{@code priority = LOW} — batch processing, no latency requirement.</li>
 *   <li>{@code targetChannel = "analytics.events"} — direct channel, no routing flexibility.</li>
 *   <li>Payload size 5–50 KB from rich behavioural context maps — strongest size-factor activator.</li>
 * </ul>
 *
 * <p>The combination of high throughput and large payloads produces the strongest
 * Kafka affinity scores of all services, validating the size and throughput factors.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyticsEventProducer {

    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    @Value("${gateway.url:http://gateway-service:8080}")
    private String gatewayUrl;

    private static final String SOURCE_SERVICE = "analytics-service";
    private static final String TARGET_CHANNEL = "analytics.events";

    private final Random    random          = new Random();
    private final AtomicLong sessionSequence = new AtomicLong(0);

    private static final String[] BROWSERS    = {"Chrome", "Firefox", "Safari", "Edge", "Opera"};
    private static final String[] OS_LIST     = {"Windows 11", "macOS 14", "Ubuntu 22.04", "iOS 17", "Android 14"};
    private static final String[] DEVICE_TYPES = {"desktop", "mobile", "tablet"};
    private static final String[] COUNTRIES   = {"US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "SG"};
    private static final String[] LANGUAGES   = {"en-US", "en-GB", "de-DE", "fr-FR", "ja-JP", "pt-BR"};

    /**
     * Generates a single analytics event and dispatches it through the gateway.
     *
     * @param routingHint  broker override for baseline experiments; {@code null} for adaptive routing
     * @return the message ID of the submitted envelope
     */
    public String emitAnalyticsEvent(BrokerTarget routingHint) {
        AnalyticsEvent event = generateAnalyticsEvent();

        try {
            byte[] payloadBytes = objectMapper.writeValueAsBytes(event);
            String payloadJson  = objectMapper.writeValueAsString(event);

            MessageEnvelope envelope = MessageEnvelope.builder()
                    .messageType(event.getEventType().name())
                    .sourceService(SOURCE_SERVICE)
                    .targetChannel(TARGET_CHANNEL)
                    .payload(objectMapper.readTree(payloadJson))
                    .payloadSizeBytes(payloadBytes.length)
                    .priority(MessagePriority.LOW)
                    .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .orderingRequired(false)
                    .routingHint(routingHint)
                    .headers(Map.of(
                            "eventId",   event.getEventId(),
                            "sessionId", event.getSessionId(),
                            "userId",    event.getUserId() != null ? event.getUserId() : "anonymous"
                    ))
                    .build();

            ResponseEntity<String> response =
                    restTemplate.postForEntity(gatewayUrl + "/api/route", envelope, String.class);

            log.debug("analytics event submitted: id={} type={} size={}B status={}",
                    envelope.getMessageId(), event.getEventType(),
                    payloadBytes.length, response.getStatusCode());

            return envelope.getMessageId();

        } catch (Exception ex) {
            log.error("Failed to emit analytics event: {}", ex.getMessage());
            return null;
        }
    }

    /**
     * Generates a realistic {@link AnalyticsEvent} with large property maps
     * to simulate the 5–50 KB payload range seen in production analytics pipelines.
     */
    public AnalyticsEvent generateAnalyticsEvent() {
        EventType type      = EventType.values()[random.nextInt(EventType.values().length)];
        String    sessionId = "sess-" + UUID.randomUUID().toString().substring(0, 12);
        boolean   loggedIn  = random.nextDouble() > 0.3;

        // Build a rich property map to inflate payload to realistic size
        Map<String, Object> properties = buildEventProperties(type);

        // A/B experiment assignments — typically 5–15 active experiments
        Map<String, String> experiments = new HashMap<>();
        for (int i = 0; i < 5 + random.nextInt(10); i++) {
            experiments.put("exp-" + (100 + i), random.nextBoolean() ? "control" : "variant-a");
        }

        // Simulate a multi-step funnel
        List<String> funnelSteps = new ArrayList<>();
        String[] steps = {"landing", "category", "product", "cart", "checkout", "payment", "confirmation"};
        int depth = 1 + random.nextInt(steps.length);
        for (int i = 0; i < depth; i++) {
            funnelSteps.add(steps[i]);
        }

        return AnalyticsEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(type)
                .sessionId(sessionId)
                .userId(loggedIn ? "USER-" + (10000 + random.nextInt(90000)) : null)
                .anonymousId("anon-" + UUID.randomUUID().toString().substring(0, 8))
                .userAgent("Mozilla/5.0 (" + OS_LIST[random.nextInt(OS_LIST.length)] + ")")
                .ipAddress(randomIp())
                .deviceType(DEVICE_TYPES[random.nextInt(DEVICE_TYPES.length)])
                .os(OS_LIST[random.nextInt(OS_LIST.length)])
                .browser(BROWSERS[random.nextInt(BROWSERS.length)])
                .country(COUNTRIES[random.nextInt(COUNTRIES.length)])
                .language(LANGUAGES[random.nextInt(LANGUAGES.length)])
                .pageUrl("https://shop.example.com/" + type.name().toLowerCase().replace("_", "/"))
                .pageTitle(type.name().replace("_", " "))
                .referrerUrl(random.nextBoolean() ? "https://google.com/search?q=example" : null)
                .viewportWidth(1024 + random.nextInt(1900))
                .viewportHeight(768  + random.nextInt(1000))
                .properties(properties)
                .experimentAssignments(experiments)
                .funnelSteps(funnelSteps)
                .timeToFirstByteMs((long)(50  + random.nextInt(450)))
                .domContentLoadedMs((long)(200 + random.nextInt(1800)))
                .pageLoadMs((long)(400 + random.nextInt(3600)))
                .occurredAt(Instant.now())
                .receivedAt(Instant.now())
                .sequenceInSession(sessionSequence.incrementAndGet())
                .build();
    }

    /**
     * Builds event-type-specific property maps with enough fields to produce
     * the target payload size range (5–50 KB).
     */
    private Map<String, Object> buildEventProperties(EventType type) {
        Map<String, Object> props = new HashMap<>();

        // Common fields present in all event types
        props.put("clientTimestamp",  Instant.now().toEpochMilli());
        props.put("timezone",         "America/New_York");
        props.put("screenColorDepth", 24);
        props.put("screenWidth",      1920);
        props.put("screenHeight",     1080);
        props.put("connectionType",   random.nextBoolean() ? "wifi" : "4g");
        props.put("performanceTier",  random.nextInt(3) + 1);

        // Add type-specific fields
        switch (type) {
            case PAGE_VIEW -> {
                props.put("loadTime",       200 + random.nextInt(2000));
                props.put("scrollDepth",    random.nextInt(100));
                props.put("timeOnPage",     random.nextInt(600));
                props.put("exitType",       random.nextBoolean() ? "bounce" : "navigate");
                // Add a content snapshot to inflate payload
                props.put("contentHash",    UUID.randomUUID().toString());
                props.put("aboveFold",      List.of("hero-banner", "nav-bar", "featured-products"));
                props.put("lazyLoaded",     List.of("recommendations", "reviews", "footer"));
            }
            case CLICK_EVENT -> {
                props.put("elementId",      "btn-" + random.nextInt(1000));
                props.put("elementClass",   "cta-button primary");
                props.put("elementText",    "Add to Cart");
                props.put("xCoordinate",    random.nextInt(1920));
                props.put("yCoordinate",    random.nextInt(1080));
                props.put("clickType",      "left");
            }
            case CONVERSION -> {
                props.put("conversionType", "purchase");
                props.put("revenue",        9.99 + random.nextDouble() * 990.0);
                props.put("currency",       "USD");
                props.put("orderId",        "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
                props.put("couponCode",     random.nextBoolean() ? "SAVE10" : null);
                props.put("paymentMethod",  random.nextBoolean() ? "card" : "paypal");
                // Products array to inflate payload
                List<Map<String, Object>> products = new ArrayList<>();
                for (int i = 0; i < 1 + random.nextInt(5); i++) {
                    products.add(Map.of(
                            "id",       "PROD-" + random.nextInt(1000),
                            "name",     "Product " + i,
                            "price",    9.99 + random.nextDouble() * 100,
                            "quantity", 1 + random.nextInt(3),
                            "category", "Electronics"
                    ));
                }
                props.put("products", products);
            }
            case SESSION_SUMMARY -> {
                props.put("sessionDuration",   30 + random.nextInt(3570));
                props.put("pageViewCount",     1  + random.nextInt(20));
                props.put("clickCount",        random.nextInt(50));
                props.put("searchCount",       random.nextInt(10));
                props.put("addToCartCount",    random.nextInt(5));
                props.put("purchaseCount",     random.nextInt(3));
                // Page sequence to inflate payload
                List<String> pages = new ArrayList<>();
                for (int i = 0; i < 1 + random.nextInt(15); i++) {
                    pages.add("https://shop.example.com/page-" + random.nextInt(100));
                }
                props.put("pageSequence", pages);
            }
            default -> props.put("detail", "Event of type " + type);
        }

        return props;
    }

    private String randomIp() {
        return random.nextInt(223) + 1 + "." +
               random.nextInt(256) + "." +
               random.nextInt(256) + "." +
               (1 + random.nextInt(254));
    }
}
