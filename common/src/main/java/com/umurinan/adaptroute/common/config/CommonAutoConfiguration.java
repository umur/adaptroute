package com.umurinan.adaptroute.common.config;

import com.umurinan.adaptroute.common.router.MessageRouter;
import com.umurinan.adaptroute.common.router.WeightedScoringRouter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring Boot auto-configuration for the common module.
 *
 * <p>Registers the {@link WeightedScoringRouter} as the default {@link MessageRouter}
 * bean unless a custom implementation is already present in the application context.
 * All service modules that include the {@code common} dependency on the classpath
 * will automatically inherit this configuration.</p>
 */
@AutoConfiguration
@EnableScheduling
@ComponentScan(basePackages = "com.umurinan.adaptroute.common")
public class CommonAutoConfiguration {

    /**
     * Registers the weighted scoring router as the default routing implementation.
     * Override by declaring a {@code @Primary @Bean} of type {@link MessageRouter}
     * in any service's configuration class.
     */
    @Bean
    @ConditionalOnMissingBean(MessageRouter.class)
    public MessageRouter messageRouter() {
        return new WeightedScoringRouter();
    }

    // ObjectMapper is provided by Spring Boot 4's JacksonAutoConfiguration
    // with full datetime (JavaTimeModule) and Jackson 3 (tools.jackson) support.
}
