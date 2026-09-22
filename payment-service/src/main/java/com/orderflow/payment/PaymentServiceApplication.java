package com.orderflow.payment;

import com.orderflow.reliability.ReliabilityConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = {"com.orderflow.payment", "com.orderflow.reliability"})
@EnableJpaRepositories(basePackages = {"com.orderflow.payment", "com.orderflow.reliability"})
@Import(ReliabilityConfiguration.class)
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
