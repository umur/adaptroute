package com.umurinan.adaptroute.notification.model;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;

/**
 * Domain model for a user notification event.
 *
 * <p>Represents a push notification, email, or SMS alert. Payload is intentionally
 * compact (under 1 KB) to simulate the low-latency, small-message workload that
 * favours RabbitMQ's push-delivery model in the scoring model.</p>
 */
@Value
@Builder
public class NotificationEvent {

    public enum NotificationType {
        EMAIL_NOTIFICATION,
        SMS_ALERT,
        PUSH_NOTIFICATION,
        IN_APP_NOTIFICATION
    }

    public enum Priority {
        CRITICAL,
        HIGH,
        NORMAL,
        LOW
    }

    String           notificationId;
    NotificationType type;
    String           recipientId;
    String           recipientContact;  // email address or phone number
    String           subject;
    String           body;
    Priority         priority;
    boolean          broadcastToAll;    // true => fanout semantics
    String           templateId;
    Map<String, String> templateParams;
    Instant          scheduledAt;
    Instant          occurredAt;
}
