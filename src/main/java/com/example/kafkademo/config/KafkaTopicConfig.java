package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic customerTopic() {
        return TopicBuilder.name("customer-topic")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic customerDeadLetterTopic() {
        return TopicBuilder.name("customer-topic.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
