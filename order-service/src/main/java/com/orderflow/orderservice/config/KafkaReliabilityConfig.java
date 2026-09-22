package com.orderflow.orderservice.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaReliabilityConfig {

    @Bean
    CommonErrorHandler orderServiceKafkaErrorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${orderflow.reliability.retry-backoff-ms:1000}") long retryBackoffMs,
            @Value("${orderflow.reliability.max-retries:2}") long maxRetries
    ) {
        var recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) ->
                        new TopicPartition(record.topic() + ".DLT", record.partition())
        );
        recoverer.setFailIfSendResultIsError(true);

        var handler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(retryBackoffMs, maxRetries)
        );

        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(
                    ConsumerRecord<?, ?> record,
                    Exception ex,
                    int deliveryAttempt
            ) {
                meterRegistry.counter(
                        "orderflow.kafka.retry.attempts",
                        "service", "order-service",
                        "topic", record.topic()
                ).increment();
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                meterRegistry.counter(
                        "orderflow.kafka.dlt.published",
                        "service", "order-service",
                        "source_topic", record.topic()
                ).increment();
            }
        });

        return handler;
    }
}
