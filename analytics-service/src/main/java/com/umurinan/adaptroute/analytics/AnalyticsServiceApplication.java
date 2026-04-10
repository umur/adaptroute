package com.umurinan.adaptroute.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Analytics Service — high-throughput, large-payload, batch-friendly event producer.
 *
 * <p>Generates rich analytical event streams (PAGE_VIEW, CLICK_EVENT, CONVERSION,
 * SESSION_SUMMARY) with large JSON payloads containing user behaviour context.
 * This workload is designed to strongly activate Kafka's size and throughput factors
 * in the scoring model.</p>
 */
@SpringBootApplication
@EnableScheduling
public class AnalyticsServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }
}
