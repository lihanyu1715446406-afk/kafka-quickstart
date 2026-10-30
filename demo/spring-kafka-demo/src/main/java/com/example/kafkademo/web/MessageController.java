package com.example.kafkademo.web;

import com.example.kafkademo.producer.MessageProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/messages")
public class MessageController {

    private final MessageProducer producer;

    public MessageController(MessageProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public Map<String, Object> send(@RequestParam String content,
                                    @RequestParam(required = false) String key,
                                    @RequestParam(defaultValue = "demo-topic") String topic) {
        producer.send(topic, key, content);
        return Map.of(
                "status", "sent",
                "topic", topic,
                "key", key == null ? "" : key,
                "content", content
        );
    }
}
