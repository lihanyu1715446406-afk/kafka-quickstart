# 04 Spring Boot 实战：写一个能跑的生产者 + 消费者

目标：从零建一个 Spring Boot 项目，提供 HTTP 接口发消息，后台消费者把消息打印出来。代码已经放在 `demo/spring-kafka-demo/`，这一章解释每一行为什么这么写。

---

## 4.1 版本要求

| 组件 | 版本 | 说明 |
|---|---|---|
| JDK | 17+ | Spring Boot 4.x 最低要求 |
| Spring Boot | 4.1.2 | 当前稳定版 |
| spring-kafka | 4.1.2 | 由 Boot 管理，不用手写版本号 |
| kafka-clients | 4.2.2 | 由 Boot 管理 |
| Kafka broker | 4.4.0 | Docker 里的那个，客户端 4.2 连 4.4 完全兼容 |

> **Spring Boot 4 的变化**：Kafka 的 starter 改名为 `spring-boot-starter-kafka`（Boot 3 是直接引 `spring-kafka`）。网上很多 Boot 3 的教程依赖写法不一样，注意区分。

---

## 4.2 项目结构

```
spring-kafka-demo/
├── pom.xml
└── src/main/
    ├── java/com/example/kafkademo/
    │   ├── KafkaDemoApplication.java    # 启动类
    │   ├── config/TopicConfig.java      # 启动时自动建 topic
    │   ├── producer/MessageProducer.java
    │   ├── consumer/MessageConsumer.java
    │   └── web/MessageController.java
    └── resources/application.yml
```

---

## 4.3 依赖：pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>4.1.2</version>
        <relativePath/>
    </parent>

    <groupId>com.example</groupId>
    <artifactId>spring-kafka-demo</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>spring-kafka-demo</name>

    <properties>
        <java.version>17</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

只引了两个 starter：

- `spring-boot-starter-kafka`：带来 spring-kafka、kafka-clients 和自动配置；
- `spring-boot-starter-web`：提供 REST 接口方便测试。

---

## 4.4 配置：application.yml

```yaml
server:
  # 用 8081，避免和本机常见的 8080 服务冲突
  port: 8081

spring:
  application:
    name: spring-kafka-demo

  kafka:
    bootstrap-servers: localhost:9092

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

    consumer:
      group-id: demo-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

    listener:
      ack-mode: batch
      missing-topics-fatal: false
```

逐项解释：

- `bootstrap-servers`：Kafka 地址。客户端只需要连上一个 broker，就能拿到整个集群的元数据。
- `key-serializer` / `value-serializer`：发送前把 Java 对象转成字节。这里用最简单的 String。
- `group-id`：消费组。**同一个组内分区不重复分配**。
- `auto-offset-reset: earliest`：组里没有已提交 offset 时，从最早开始读。注意：**只在没有 offset 时生效**。
- `ack-mode: batch`：Spring 的监听容器在一批消息处理完后提交 offset。这是 Spring 的默认行为，和原生客户端的 `enable.auto.commit` 不是一回事。
- `missing-topics-fatal: false`：topic 暂时不存在也不要让应用启动失败。

---

## 4.5 启动类

```java
package com.example.kafkademo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }
}
```

`@EnableKafka` 开启 `@KafkaListener` 注解的解析。Spring Boot 通常会自动帮你开启，但显式写上更清楚，也不会冲突。

---

## 4.6 启动时创建 topic

```java
package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

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
```

Spring Boot 检测到 `NewTopic` 类型的 Bean 后，会通过 KafkaAdmin 在 broker 上创建它。已经存在则跳过。

> 单机环境副本数必须是 1，写 3 会创建失败。

生产环境更推荐用运维脚本/平台建 topic，把分区数、副本数、保留策略纳入版本管理；`NewTopic` Bean 适合本地开发和 demo。

---

## 4.7 生产者

```java
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
```

`KafkaTemplate` 是 Spring 对 KafkaProducer 的封装，已经由 Boot 自动配置好，直接注入即可。

`send` 是**异步**的，返回 `CompletableFuture`。生产环境可以加 `.whenComplete(...)` 记录成功/失败，或者用同步的 `.get()`（会阻塞，慎用）。

---

## 4.8 消费者

```java
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
```

`@KafkaListener` 背后是 Spring 启动的一个**监听容器**：它在后台循环 `poll` 消息、反序列化、调用你的方法、按 `ack-mode` 提交 offset。你不需要自己写 while 循环。

方法参数可以直接是 `String`（只关心 value），也可以是 `ConsumerRecord`（需要 key/partition/offset 时）。这里用 `ConsumerRecord` 方便观察。

---

## 4.9 REST 接口

```java
package com.example.kafkademo.web;

import com.example.kafkademo.producer.MessageProducer;
import org.springframework.web.bind.annotation.*;

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
```

---

## 4.10 运行和验证

### 第一步：确认 Kafka 在跑

```powershell
cd demo
docker compose ps
```

### 第二步：切到 JDK 17 并启动应用

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"   # 换成你自己的 JDK 17 安装路径
$env:Path = "$env:JAVA_HOME\bin;$env:Path"

cd demo\spring-kafka-demo     # 从仓库根目录进入
mvn spring-boot:run
```

第一次会下载依赖，耐心等。看到下面的日志就启动成功了：

```
Tomcat started on port 8081
Started KafkaDemoApplication in x.xxx seconds
```

（`Tomcat started` 是 Spring Boot 4 内嵌 Web 服务器的日志，端口 8081。这里不用 8080，是为了避开本机常见的其他 Web 服务，端口冲突时改 `application.yml` 即可。）

### 第三步：另开一个终端发消息

```powershell
curl.exe -X POST "http://localhost:8081/messages?content=hello-kafka"
```

返回：

```json
{"status":"sent","topic":"demo-topic","key":"","content":"hello-kafka"}
```

回到应用控制台，应该能看到：

```
收到消息 -> partition=0, offset=0, key=null, value=hello-kafka
```

### 第四步：观察 key 和分区的关系

```powershell
curl.exe -X POST "http://localhost:8081/messages?content=order-created&key=order-1"
curl.exe -X POST "http://localhost:8081/messages?content=order-paid&key=order-1"
curl.exe -X POST "http://localhost:8081/messages?content=order-shipped&key=order-1"
```

三条消息的 key 都是 `order-1`，观察日志里的 `partition`——**一定相同**。这就是 Kafka 保证同一业务消息有序的方式。

### 第五步：用命令行独立验证

用另一个消费组从头读一遍，证明消息确实在 Kafka 里：

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic --group cli-check --from-beginning --property print.key=true --property print.partition=true
```

`cli-check` 是新的组，所以能从头读到所有消息，和 `demo-group` 互不影响。

---

## 4.11 背后发生了什么

1. **自动配置**：`spring-boot-starter-kafka` 检测到配置，创建 `KafkaTemplate`、`ConsumerFactory`、`KafkaListenerContainerFactory`、`KafkaAdmin`。
2. **建 topic**：`KafkaAdmin` 发现 `NewTopic` Bean，调用 AdminClient 创建 `demo-topic`（3 分区）。
3. **发消息**：Controller 收到请求 → `KafkaTemplate.send` → 序列化 → 选分区 → 攒批 → 发到 broker。
4. **消费**：监听容器后台 poll → 反序列化 → 调 `consume()` → 一批处理完提交 offset。
5. **offset 存在哪**：Kafka 内部的 `__consumer_offsets` topic，所以重启应用后能从上次位置继续。

> 默认 `ack-mode: batch` 意味着「先处理、后提交」。如果应用在处理完但提交前崩溃，重启后会重复消费——这就是 **at-least-once**。业务要按幂等设计（比如用订单号 + 状态机去重）。

---

## 4.12 进阶用法

### 手动提交 offset

配置：

```yaml
spring:
  kafka:
    listener:
      ack-mode: manual_immediate
```

消费者方法加 `Acknowledgment` 参数，处理成功后手动确认：

```java
import org.springframework.kafka.support.Acknowledgment;

@KafkaListener(topics = "demo-topic", groupId = "demo-group")
public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
    // 业务处理
    log.info("处理: {}", record.value());
    ack.acknowledge();   // 处理成功才提交
}
```

适合「宁可重复，不能丢」的场景。注意：如果业务处理抛异常且没 catch，消息不会提交，会触发重试。

### 发送 JSON

Boot 4 / Spring Kafka 4 里，旧的 `JsonSerializer` 已标记为过时，新类是 `JacksonJsonSerializer` / `JacksonJsonDeserializer`。配置示例：

```yaml
spring:
  kafka:
    producer:
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
    consumer:
      value-deserializer: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafkademo"
```

同时把消费者方法的参数改成你的 DTO 类型。Boot 4 需要 Jackson 3，确认 classpath 里有 `spring-boot-starter-jackson`（`starter-web` 一般会带）。

**初学者建议先用 String 发 JSON 字符串**，把 Kafka 的机制跑通，再上自动序列化——少一层魔法，出错时更容易定位。

### 提高消费并发

```yaml
spring:
  kafka:
    listener:
      concurrency: 3
```

每个并发对应一个消费者线程。**并发数不要超过分区数**，否则多出来的线程分不到分区。

### 指定分区发送

```java
kafkaTemplate.send(topic, partition, key, message);
```

一般不需要手动指定，交给 key 哈希更稳。

### 消费失败怎么办

- 临时故障（下游超时）：让异常抛出，Spring 的重试机制会重试；
- 永久故障（脏数据）：重试多次后仍失败，应该进**死信 topic（DLT）**，人工处理；
- Spring Kafka 提供 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`，可以搜索这两个类继续深入。

---

## 4.13 排错

| 现象 | 原因 / 解决 |
|---|---|
| 启动报 `Class file has wrong version 61.0` | 还在用 JDK 11 编译。确认 `mvn -v` 显示 17，或 IDEA 的 Project SDK 改成 17 |
| `Connection to node -1 ...` / 发送一直重试 | Kafka 没启动，或 `bootstrap-servers` 地址不对 |
| 消费者收不到消息 | 检查 `group-id`、topic 名、`auto-offset-reset`；换个新 group 试 `--from-beginning` |
| 每次重启都重复消费 | 正常（at-least-once）。检查业务是否幂等，或改用 `manual_immediate` |
| 启动报 8081 端口占用 | 改 `server.port`，或停掉占用进程 |
| 启动很慢（约 40 秒），日志里有 `Could not configure topics` | Kafka 没启动，KafkaAdmin 在建 topic 时超时。先 `docker compose up -d` 再启动应用 |
| `missing-topics-fatal` 报错 | 已配置为 false；仍报错说明 Kafka 连不上，不是 topic 问题 |
| IDEA 里 `@KafkaListener` 不生效 | 检查是否有 `@EnableKafka`、类是否被 Spring 扫描到（在启动类同包或子包下） |

---

## 本章验收

- [ ] `mvn spring-boot:run` 能启动，控制台出现 `Started KafkaDemoApplication`
- [ ] POST 一条消息后，控制台打印 partition / offset / key / value
- [ ] 同一个 key 的三条消息落在同一个 partition
- [ ] 用 `cli-check` 组能从命令行读到同样的消息
- [ ] 能解释 `ack-mode: batch` 和"可能重复消费"的关系

搞定这些，你已经能在一个真实 Spring Boot 项目里使用 Kafka 了。下一章讲选型和优缺点。
