package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 应用启动时由 KafkaAdmin 自动创建 topic（已存在则跳过）。
 * 单机环境副本数必须是 1。
 */
@Configuration
public class TopicConfig {

    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder.name("demo-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
