package com.example.kafkademo.consumer;

import com.example.kafkademo.model.Customer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaConsumer {

    @KafkaListener(topics = "customer-topic", groupId = "customer-group")
    public void consume(Customer customer) {
        System.out.println("Received customer: " + customer);

        if (customer == null || customer.getEmail() == null || !customer.getEmail().contains("@")) {
            throw new RuntimeException("Invalid customer email");
        }

        System.out.println("Customer processed successfully");
    }
}
