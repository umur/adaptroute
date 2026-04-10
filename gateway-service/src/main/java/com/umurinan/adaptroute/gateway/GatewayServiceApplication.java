package com.umurinan.adaptroute.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Gateway Service.
 *
 * <p>The gateway service is the central hub of the adaptive routing system. It:</p>
 * <ul>
 *   <li>Accepts {@link com.umurinan.adaptroute.common.dto.MessageEnvelope} submissions
 *       from producer services via REST.</li>
 *   <li>Runs the {@link com.umurinan.adaptroute.common.router.WeightedScoringRouter}
 *       to determine the target broker for each message.</li>
 *   <li>Dispatches to Apache Kafka or RabbitMQ via the
 *       {@link com.umurinan.adaptroute.gateway.router.AdaptiveMessageDispatcher}.</li>
 *   <li>Consumes messages from both brokers for end-to-end latency measurement.</li>
 *   <li>Exports routing metrics via {@code /actuator/prometheus}.</li>
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
public class GatewayServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayServiceApplication.class, args);
    }
}
