package com.example.kafkademo.producer;

import com.example.kafkademo.model.Customer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaProducer {

    private final KafkaTemplate<String, Customer> kafkaTemplate;

    public JsonKafkaProducer(KafkaTemplate<String, Customer> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Customer customer) {
        System.out.println("Sending customer: " + customer);
        kafkaTemplate.send("customer-topic", customer);
    }
}
