package com.orderflow.reliability;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class ReliabilityConfiguration {

    @Bean
    OutboxService outboxService(
            OutboxMessageRepository repository,
            ObjectMapper objectMapper
    ) {
        return new OutboxService(repository, objectMapper);
    }

    @Bean
    OutboxPublisher outboxPublisher(
            OutboxMessageRepository repository,
            ObjectMapper objectMapper,
            KafkaTemplate<String, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${orderflow.reliability.publish-timeout-ms:10000}") long publishTimeoutMs
    ) {
        return new OutboxPublisher(
                repository,
                objectMapper,
                kafkaTemplate,
                meterRegistry,
                publishTimeoutMs
        );
    }

    @Bean
    CommonErrorHandler orderflowCommonErrorHandler(
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
                        "topic", record.topic()
                ).increment();
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                meterRegistry.counter(
                        "orderflow.kafka.dlt.published",
                        "source_topic", record.topic()
                ).increment();
            }
        });

        return handler;
    }
}
