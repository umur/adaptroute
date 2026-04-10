package com.umurinan.adaptroute.order.model;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Domain model for an order lifecycle event.
 *
 * <p>Carries realistic e-commerce order data: customer, line items, payment info,
 * and fulfilment status. The rich payload structure produces messages in the
 * 2–20 KB range, which combined with high emit rates creates a workload that
 * strongly favours Kafka in the scoring model.</p>
 */
@Value
@Builder
public class OrderEvent {

    public enum EventType {
        ORDER_CREATED,
        ORDER_PAYMENT_PROCESSED,
        ORDER_SHIPPED,
        ORDER_COMPLETED,
        ORDER_CANCELLED
    }

    String        orderId;
    EventType     eventType;
    String        customerId;
    String        customerEmail;
    List<LineItem> lineItems;
    BigDecimal    totalAmount;
    String        currency;
    String        shippingAddress;
    String        paymentMethod;
    String        status;
    Instant       occurredAt;
    int           sequenceNumber; // used to verify ordering at the consumer

    @Value
    @Builder
    public static class LineItem {
        String     productId;
        String     productName;
        int        quantity;
        BigDecimal unitPrice;
        String     category;
    }
}
