# Kafka 快速上手教程

> 一份写给「会一点 Java、用过 Spring Boot」的人的 Kafka 入门教程：**从零把 Kafka 跑起来，到能写生产者/消费者，再到知道什么时候该用它。**

![Kafka](https://img.shields.io/badge/Apache%20Kafka-4.4.0-black)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.2-6DB33F)
![License](https://img.shields.io/badge/License-MIT-green)

- **运行环境**：Docker Desktop、JDK 17+、Maven 3.9+
- **示例平台**：Windows + PowerShell（Linux / macOS 命令基本通用）
- **对应版本**：Apache Kafka 4.4.0（KRaft，无需 ZooKeeper）、Spring Boot 4.1.2

---

## 先看这三句话

1. **Kafka 是一个「只能追加、可以重复读」的分布式日志**，不是传统意义上「读完就删」的队列。
2. **生产者写进去，消费者按自己的进度读**。每个消费者组自己记一个 offset（读到哪了），互不干扰。
3. **Kafka 快，是因为顺序写磁盘 + 批量 + 零拷贝**；Kafka 难，主要难在「分区、副本、消费组」这几个概念和运维。

这三句能用自己的话讲出来，入门就成功了一半。

---

## 适合谁

**适合**：会一点 Java、用过 Spring Boot，想尽快把 Kafka 跑起来并用起来的人。

**不适合**：想深入 Kafka 内核（ISR 协议、日志压缩、Controller 选举细节）的人——第 06 章文末给了进阶方向。

---

## 目录

| 章节 | 内容 | 预计时间 |
|---|---|---|
| [01 环境搭建](01-环境搭建.md) | 用 Docker 起一个单机 Kafka，验证能连上 | 30~60 分钟（含下载） |
| [02 核心概念](02-核心概念.md) | Topic / Partition / Offset / 消费组 / 副本，一次讲透 | 60~90 分钟 |
| [03 命令行操作](03-命令行操作.md) | 建 topic、发消息、收消息、看消费组、重置 offset | 40 分钟（边敲边学） |
| [04 Spring Boot 实战](04-Spring-Boot实战.md) | 写一个能跑的生产者 + 消费者 + REST 接口 | 60~90 分钟 |
| [05 优缺点与选型](05-优缺点与选型.md) | 什么时候该用 Kafka，和 RabbitMQ / RocketMQ 怎么选 | 30 分钟 |
| [06 常见问题与进阶](06-常见问题与进阶.md) | 踩坑清单、面试高频问题、下一步学什么 | 随时查 |

配套代码在 [`demo/`](demo/)：

```
demo/
├── docker-compose.yml          # 最简启动文件（适合日常实验）
├── docker-compose.full.yml     # 显式配置 + 数据卷 + 健康检查（适合想看清每个参数的人）
└── spring-kafka-demo/          # Spring Boot 可运行示例
```

---

## 快速开始（约 3 分钟）

前提：已安装 Docker Desktop。

```powershell
# 1. 在项目根目录启动 Kafka
cd demo
docker compose up -d

# 2. 确认 Kafka 活着（没有报错、没有输出就是正常）
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 3. 发一条消息（另开一个终端）
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-topic
```

跑 Spring Boot 示例：

```powershell
cd demo/spring-kafka-demo
mvn spring-boot:run
```

启动后用 curl 发一条消息，控制台会打印消费者收到的内容：

```powershell
curl.exe -X POST "http://localhost:8081/messages?content=hello-kafka"
```

接口支持三个参数：`content`（必填）、`key`（可选，决定分区）、`topic`（默认 `demo-topic`）。

完整步骤和每个参数的含义见 [01 环境搭建](01-环境搭建.md) 和 [04 Spring Boot 实战](04-Spring-Boot实战.md)。

---

## 推荐学习顺序

**第一次学（约 4~6 小时）**

1. 按 01 把 Kafka 跑起来，看到 `kafka-topics.sh --list` 能返回结果
2. 读 02，重点搞懂「分区 + offset + 消费组」三件套
3. 按 03 亲手建 topic、收发消息、看 LAG
4. 按 04 跑通 Spring Boot demo，用 curl 发一条消息并在控制台看到
5. 读 05、06，建立选型判断

**只想 30 分钟速通**

读 01 的 1.3~1.5 节 → 02 的「一张图看懂」→ 03 的前 4 条命令 → 05 的对比表。

---

## 学完的自测清单

能不看文档回答出来，就算入门了：

- [ ] Topic 和 Partition 是什么关系？为什么一个 topic 要分多个 partition？
- [ ] offset 是什么？为什么不同消费组可以各自读到同一条消息？
- [ ] 一个消费组里消费者数量超过分区数会怎样？
- [ ] Kafka 能保证全局消息顺序吗？怎样才能保证同一业务的消息有序？
- [ ] 消息被消费后会被删除吗？默认保留多久？
- [ ] `auto.offset.reset=earliest` 在什么情况下才生效？
- [ ] 副本（replica）和 ISR 解决什么问题？`acks=all` 意味着什么？
- [ ] Kafka 4.x 还需要 ZooKeeper 吗？

答案都能在 02、03、05 里找到。

---

## 环境准备（一句话版本）

```powershell
cd demo
docker compose up -d
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

第二条命令没有报错、没有输出，就说明 Kafka 已经活着了。详细步骤见 [01 环境搭建](01-环境搭建.md)。

> 国内网络拉取镜像较慢时，可以配置 Docker 镜像加速或代理；Kafka 本身不需要本机 Java，第 04 章的 Spring Boot 示例才需要 JDK 17。

---

## 许可

本项目采用 [MIT License](LICENSE)，文档和示例代码都可以自由使用、修改、再分发，保留版权声明即可。

欢迎提 Issue 或 PR 指出错误、补充内容。
