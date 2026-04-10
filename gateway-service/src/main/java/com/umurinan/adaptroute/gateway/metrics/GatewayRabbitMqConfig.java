package com.umurinan.adaptroute.gateway.metrics;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ topology declaration for the gateway service.
 *
 * <p>Declares a topic exchange ({@code adaptive.exchange}) and a durable queue
 * ({@code adaptive.queue}) bound with a wildcard routing key ({@code #}).
 * The wildcard binding ensures the gateway consumer receives all messages
 * regardless of the routing key set by the dispatcher.</p>
 *
 * <p>A topic exchange is chosen over fanout to support the routing-flexibility
 * factor in the scoring model: individual producer services can use channel-specific
 * routing keys (e.g., {@code broadcast.notifications.#}) for selective consumption.</p>
 */
@Configuration
public class GatewayRabbitMqConfig {

    public static final String EXCHANGE_NAME = "adaptive.exchange";
    public static final String QUEUE_NAME    = "adaptive.queue";
    public static final String BINDING_KEY   = "#";

    @Bean
    public TopicExchange adaptiveExchange() {
        return new TopicExchange(EXCHANGE_NAME, true, false);
    }

    @Bean
    public Queue adaptiveQueue() {
        return new Queue(QUEUE_NAME, true);
    }

    @Bean
    public Binding adaptiveBinding(Queue adaptiveQueue, TopicExchange adaptiveExchange) {
        return BindingBuilder.bind(adaptiveQueue)
                .to(adaptiveExchange)
                .with(BINDING_KEY);
    }

    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                          Jackson2JsonMessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        // Enable publisher confirms for at-least-once delivery tracking
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                // In production: schedule for retry or send to dead-letter queue
                System.err.println("RabbitMQ publisher confirm NACK: " + cause);
            }
        });
        return template;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            Jackson2JsonMessageConverter messageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setConcurrentConsumers(2);
        factory.setMaxConcurrentConsumers(8);
        factory.setPrefetchCount(50);
        return factory;
    }
}
