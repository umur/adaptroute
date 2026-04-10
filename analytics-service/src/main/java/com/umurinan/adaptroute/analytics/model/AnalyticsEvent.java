package com.umurinan.adaptroute.analytics.model;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Domain model for a user analytics event.
 *
 * <p>Carries rich behavioural context: device fingerprint, session data, A/B test
 * assignments, and custom property bags. Payloads range from 5–50 KB, making this
 * the heaviest message type in the system and the strongest activator of Kafka's
 * size factor in the scoring model.</p>
 */
@Value
@Builder
public class AnalyticsEvent {

    public enum EventType {
        PAGE_VIEW,
        CLICK_EVENT,
        CONVERSION,
        SESSION_SUMMARY,
        SEARCH_QUERY,
        PRODUCT_VIEW,
        ADD_TO_CART,
        CHECKOUT_STARTED
    }

    String    eventId;
    EventType eventType;
    String    sessionId;
    String    userId;
    String    anonymousId;

    // Device & environment context
    String    userAgent;
    String    ipAddress;
    String    deviceType;    // desktop, mobile, tablet
    String    os;
    String    browser;
    String    country;
    String    language;

    // Page / screen context
    String    pageUrl;
    String    pageTitle;
    String    referrerUrl;
    int       viewportWidth;
    int       viewportHeight;

    // Event-specific properties (flexible schema)
    Map<String, Object> properties;

    // A/B test variants active during this event
    Map<String, String> experimentAssignments;

    // Funnel position for conversion analysis
    List<String> funnelSteps;

    // Performance metrics (for PAGE_VIEW events)
    Long timeToFirstByteMs;
    Long domContentLoadedMs;
    Long pageLoadMs;

    Instant occurredAt;
    Instant receivedAt;
    long    sequenceInSession;
}
