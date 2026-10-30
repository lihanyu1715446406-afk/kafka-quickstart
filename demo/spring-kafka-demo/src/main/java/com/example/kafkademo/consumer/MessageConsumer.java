package com.example.kafkademo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = "demo-topic", groupId = "demo-group")
    public void consume(ConsumerRecord<String, String> record) {
        log.info("收到消息 -> partition={}, offset={}, key={}, value={}",
                record.partition(),
                record.offset(),
                record.key(),
                record.value());

        // 这里写你的业务逻辑：写数据库、调用下游、发通知……
    }
}
