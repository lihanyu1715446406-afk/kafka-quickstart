package com.example.kafkademo.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, String>> send(String topic, String key, String message) {
        if (key == null || key.isBlank()) {
            // 没有 key：由 Kafka 的粘性分区器决定分区
            return kafkaTemplate.send(topic, message);
        }
        // 有 key：hash(key) % 分区数，同一个 key 永远进同一个分区
        return kafkaTemplate.send(topic, key, message);
    }
}
