package com.umurinan.adaptroute.experiments.runner;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Spring configuration for the experiment runner module.
 */
@Configuration
public class ExperimentConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    // ObjectMapper is provided by Spring Boot 4's JacksonAutoConfiguration
}
