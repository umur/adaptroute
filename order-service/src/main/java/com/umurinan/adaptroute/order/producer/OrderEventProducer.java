package com.umurinan.adaptroute.order.producer;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.DeliveryGuarantee;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.MessagePriority;
import com.umurinan.adaptroute.order.model.OrderEvent;
import com.umurinan.adaptroute.order.model.OrderEvent.EventType;
import com.umurinan.adaptroute.order.model.OrderEvent.LineItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Produces realistic order lifecycle events and submits them to the gateway router.
 *
 * <p>Each emitted {@link MessageEnvelope} has:</p>
 * <ul>
 *   <li>{@code orderingRequired = true} — activates Kafka's ordering factor.</li>
 *   <li>{@code deliveryGuarantee = EXACTLY_ONCE} — activates Kafka's delivery guarantee factor.</li>
 *   <li>{@code priority = NORMAL} — neutral priority (no RabbitMQ latency bias).</li>
 *   <li>{@code targetChannel = "orders"} — direct routing key, minimal flexibility factor.</li>
 *   <li>Payload size 2–20 KB from realistic order JSON.</li>
 * </ul>
 *
 * <p>These characteristics produce a consistently high Kafka composite score,
 * making the order service the primary Kafka-affinity use case in the paper.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventProducer {

    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    @Value("${gateway.url:http://gateway-service:8080}")
    private String gatewayUrl;

    private static final String SOURCE_SERVICE = "order-service";
    private static final String TARGET_CHANNEL = "orders";

    private final AtomicInteger sequenceCounter = new AtomicInteger(0);
    private final Random random = new Random();

    // Product catalogue used for realistic line-item generation
    private static final List<String[]> PRODUCTS = List.of(
            new String[]{"PROD-001", "Wireless Headphones",    "Electronics"},
            new String[]{"PROD-002", "Running Shoes",          "Footwear"},
            new String[]{"PROD-003", "Coffee Maker",           "Appliances"},
            new String[]{"PROD-004", "Yoga Mat",               "Sports"},
            new String[]{"PROD-005", "Mechanical Keyboard",    "Electronics"},
            new String[]{"PROD-006", "Desk Lamp",              "Office"},
            new String[]{"PROD-007", "Water Bottle",           "Sports"},
            new String[]{"PROD-008", "Backpack",               "Accessories"},
            new String[]{"PROD-009", "Smart Watch",            "Electronics"},
            new String[]{"PROD-010", "Standing Desk",          "Office"}
    );

    private static final String[] COUNTRIES = {
            "US", "GB", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "SG"
    };

    /**
     * Generates a single order event and dispatches it through the gateway.
     *
     * @param routingHint  broker override for baseline experiments; {@code null} for adaptive routing
     * @return the message ID of the submitted envelope
     */
    public String emitOrderEvent(BrokerTarget routingHint) {
        OrderEvent event = generateOrderEvent();

        try {
            byte[] payloadBytes = objectMapper.writeValueAsBytes(event);
            String payloadJson  = objectMapper.writeValueAsString(event);

            MessageEnvelope envelope = MessageEnvelope.builder()
                    .messageType(event.getEventType().name())
                    .sourceService(SOURCE_SERVICE)
                    .targetChannel(TARGET_CHANNEL)
                    .payload(objectMapper.readTree(payloadJson))
                    .payloadSizeBytes(payloadBytes.length)
                    .priority(MessagePriority.NORMAL)
                    .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .orderingRequired(true)
                    .routingHint(routingHint)
                    .headers(Map.of(
                            "orderId",      event.getOrderId(),
                            "customerId",   event.getCustomerId(),
                            "sequenceNum",  String.valueOf(event.getSequenceNumber())
                    ))
                    .build();

            ResponseEntity<String> response =
                    restTemplate.postForEntity(gatewayUrl + "/api/route", envelope, String.class);

            log.debug("order event submitted: id={} type={} size={}B status={}",
                    envelope.getMessageId(), event.getEventType(),
                    payloadBytes.length, response.getStatusCode());

            return envelope.getMessageId();

        } catch (Exception ex) {
            log.error("Failed to emit order event {}: {}", event.getOrderId(), ex.getMessage());
            return null;
        }
    }

    /**
     * Generates a realistic {@link OrderEvent} with randomised but plausible data.
     */
    public OrderEvent generateOrderEvent() {
        String orderId    = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String customerId = "CUST-" + (10000 + random.nextInt(90000));
        int    itemCount  = 1 + random.nextInt(6);

        List<LineItem> lineItems = new ArrayList<>();
        BigDecimal total = BigDecimal.ZERO;

        for (int i = 0; i < itemCount; i++) {
            String[] product  = PRODUCTS.get(random.nextInt(PRODUCTS.size()));
            int      qty      = 1 + random.nextInt(4);
            BigDecimal price  = BigDecimal.valueOf(9.99 + random.nextDouble() * 490.0)
                    .setScale(2, java.math.RoundingMode.HALF_UP);

            lineItems.add(LineItem.builder()
                    .productId(product[0])
                    .productName(product[1])
                    .quantity(qty)
                    .unitPrice(price)
                    .category(product[2])
                    .build());

            total = total.add(price.multiply(BigDecimal.valueOf(qty)));
        }

        String country = COUNTRIES[random.nextInt(COUNTRIES.length)];
        EventType[] types = EventType.values();

        return OrderEvent.builder()
                .orderId(orderId)
                .eventType(types[random.nextInt(types.length)])
                .customerId(customerId)
                .customerEmail(customerId.toLowerCase() + "@example.com")
                .lineItems(lineItems)
                .totalAmount(total.setScale(2, java.math.RoundingMode.HALF_UP))
                .currency("USD")
                .shippingAddress(
                        (100 + random.nextInt(9900)) + " Main St, " + country)
                .paymentMethod(random.nextBoolean() ? "CREDIT_CARD" : "PAYPAL")
                .status("PENDING")
                .occurredAt(Instant.now())
                .sequenceNumber(sequenceCounter.incrementAndGet())
                .build();
    }
}
