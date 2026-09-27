package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterConsumer {

    @KafkaListener(
            topics = "customer-topic.DLT",
            groupId = "customer-dlt-group",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Object> record) {
        System.out.println("Failed message received in DLT");
        System.out.println("topic=" + record.topic()
                + ", partition=" + record.partition()
                + ", offset=" + record.offset()
                + ", key=" + record.key()
                + ", value=" + record.value());

        for (Header header : record.headers()) {
            if (header.key().contains("exception") || header.key().contains("original")) {
                System.out.println("header=" + header.key() + ": " + new String(header.value()));
            }
        }
    }
}
