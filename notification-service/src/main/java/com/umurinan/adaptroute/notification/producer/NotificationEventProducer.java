package com.umurinan.adaptroute.notification.producer;

import tools.jackson.databind.ObjectMapper;
import com.umurinan.adaptroute.common.dto.MessageEnvelope;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.BrokerTarget;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.DeliveryGuarantee;
import com.umurinan.adaptroute.common.dto.MessageEnvelope.MessagePriority;
import com.umurinan.adaptroute.notification.model.NotificationEvent;
import com.umurinan.adaptroute.notification.model.NotificationEvent.NotificationType;
import com.umurinan.adaptroute.notification.model.NotificationEvent.Priority;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Produces user notification events and submits them to the gateway router.
 *
 * <p>Each emitted {@link MessageEnvelope} has:</p>
 * <ul>
 *   <li>{@code orderingRequired = false} — no ordering benefit from Kafka.</li>
 *   <li>{@code deliveryGuarantee = AT_LEAST_ONCE} — activates RabbitMQ's delivery factor.</li>
 *   <li>{@code priority = HIGH or CRITICAL} — activates RabbitMQ's latency factor.</li>
 *   <li>{@code targetChannel = "broadcast.notifications"} — activates RabbitMQ's routing flexibility factor.</li>
 *   <li>Payload size 200–800 bytes — activates RabbitMQ's ackSpeed factor (small = fast confirm).</li>
 * </ul>
 *
 * <p>These characteristics produce a consistently high RabbitMQ composite score,
 * making the notification service the primary RabbitMQ-affinity use case.</p>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationEventProducer {

    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    @Value("${gateway.url:http://gateway-service:8080}")
    private String gatewayUrl;

    private static final String SOURCE_SERVICE = "notification-service";
    private static final String TARGET_CHANNEL = "broadcast.notifications";

    private final Random random = new Random();

    private static final List<String[]> EMAIL_TEMPLATES = List.of(
            new String[]{"TMPL-ORDER-CONFIRM",  "Your order has been confirmed"},
            new String[]{"TMPL-SHIP-NOTIF",     "Your order has shipped"},
            new String[]{"TMPL-PROMO-WEEKLY",   "This week's deals just for you"},
            new String[]{"TMPL-ACCOUNT-ALERT",  "Unusual activity on your account"},
            new String[]{"TMPL-WELCOME",        "Welcome to our platform"}
    );

    private static final List<String> SMS_MESSAGES = List.of(
            "Your verification code is: %06d",
            "Order #%s shipped. Track at track.example.com/%s",
            "Payment of $%.2f confirmed",
            "Your appointment is tomorrow at %d:00",
            "Flash sale: 30%% off until midnight"
    );

    /**
     * Generates a single notification event and dispatches it through the gateway.
     *
     * @param routingHint  broker override for baseline experiments; {@code null} for adaptive routing
     * @return the message ID of the submitted envelope
     */
    public String emitNotificationEvent(BrokerTarget routingHint) {
        NotificationEvent event = generateNotificationEvent();

        try {
            byte[] payloadBytes = objectMapper.writeValueAsBytes(event);
            String payloadJson  = objectMapper.writeValueAsString(event);

            // Derive envelope priority from notification priority
            MessagePriority envPriority = switch (event.getPriority()) {
                case CRITICAL -> MessagePriority.CRITICAL;
                case HIGH     -> MessagePriority.HIGH;
                case NORMAL   -> MessagePriority.NORMAL;
                case LOW      -> MessagePriority.LOW;
            };

            MessageEnvelope envelope = MessageEnvelope.builder()
                    .messageType(event.getType().name())
                    .sourceService(SOURCE_SERVICE)
                    .targetChannel(TARGET_CHANNEL)
                    .payload(objectMapper.readTree(payloadJson))
                    .payloadSizeBytes(payloadBytes.length)
                    .priority(envPriority)
                    .deliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .orderingRequired(false)
                    .routingHint(routingHint)
                    .headers(Map.of(
                            "notificationId", event.getNotificationId(),
                            "recipientId",    event.getRecipientId(),
                            "broadcast",      String.valueOf(event.isBroadcastToAll())
                    ))
                    .build();

            ResponseEntity<String> response =
                    restTemplate.postForEntity(gatewayUrl + "/api/route", envelope, String.class);

            log.debug("notification event submitted: id={} type={} size={}B priority={} status={}",
                    envelope.getMessageId(), event.getType(),
                    payloadBytes.length, envPriority, response.getStatusCode());

            return envelope.getMessageId();

        } catch (Exception ex) {
            log.error("Failed to emit notification event: {}", ex.getMessage());
            return null;
        }
    }

    /**
     * Generates a realistic {@link NotificationEvent}.
     */
    public NotificationEvent generateNotificationEvent() {
        NotificationType type     = NotificationType.values()[random.nextInt(NotificationType.values().length)];
        Priority         priority = random.nextDouble() < 0.2 ? Priority.HIGH
                : (random.nextDouble() < 0.05 ? Priority.CRITICAL : Priority.NORMAL);

        String recipientId = "USER-" + (10000 + random.nextInt(90000));
        String[] template  = EMAIL_TEMPLATES.get(random.nextInt(EMAIL_TEMPLATES.size()));

        return NotificationEvent.builder()
                .notificationId(UUID.randomUUID().toString())
                .type(type)
                .recipientId(recipientId)
                .recipientContact(switch (type) {
                    case EMAIL_NOTIFICATION  -> recipientId.toLowerCase() + "@example.com";
                    case SMS_ALERT           -> "+1555" + (1000000 + random.nextInt(9000000));
                    default                  -> recipientId;
                })
                .subject(template[1])
                .body("Dear customer, " + template[1] + ". Please take action if required.")
                .priority(priority)
                .broadcastToAll(type == NotificationType.PUSH_NOTIFICATION && random.nextBoolean())
                .templateId(template[0])
                .templateParams(Map.of(
                        "recipientName", "Customer " + recipientId,
                        "timestamp",     Instant.now().toString()
                ))
                .scheduledAt(Instant.now())
                .occurredAt(Instant.now())
                .build();
    }
}
