package com.orderflow.orchestrator;

import com.orderflow.reliability.ReliabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = {"com.orderflow.orchestrator", "com.orderflow.reliability"})
@EnableJpaRepositories(basePackages = {"com.orderflow.orchestrator", "com.orderflow.reliability"})
@Import(ReliabilityConfiguration.class)
public class SagaOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaOrchestratorApplication.class, args);
    }
}
