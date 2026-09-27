package com.example.kafkademo.controller;

import com.example.kafkademo.model.Customer;
import com.example.kafkademo.producer.JsonKafkaProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka")
public class CustomerController {

    private final JsonKafkaProducer producer;

    public CustomerController(JsonKafkaProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/publish-customer")
    public ResponseEntity<String> publishCustomer(@RequestBody Customer customer) {
        producer.sendMessage(customer);
        return ResponseEntity.accepted().body("Customer sent successfully");
    }
}
