package com.umurinan.adaptroute.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Order Service — high-throughput, ordering-critical event producer.
 *
 * <p>Generates a realistic stream of e-commerce order lifecycle events:
 * ORDER_CREATED, ORDER_PAYMENT_PROCESSED, ORDER_SHIPPED, ORDER_COMPLETED.
 * This workload is designed to maximize Kafka affinity scores due to its
 * high throughput and strict ordering requirements.</p>
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
