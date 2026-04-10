package com.umurinan.adaptroute.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Notification Service — low-latency, fanout event producer.
 *
 * <p>Generates user notification events (EMAIL_NOTIFICATION, SMS_ALERT, PUSH_NOTIFICATION)
 * with broadcast channel semantics. This workload is designed to maximize RabbitMQ
 * affinity scores due to its latency sensitivity and fanout routing requirements.</p>
 */
@SpringBootApplication
@EnableScheduling
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
