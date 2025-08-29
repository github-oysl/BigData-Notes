# Kafka基础示例项目

## 项目简介

本项目是一个Kafka基础使用示例，专为Kafka初学者设计。通过实际的代码示例，帮助开发者理解和掌握Kafka的核心概念和基本使用方法。

## 项目结构

```
kafka-basis/
├── pom.xml                                    # Maven配置文件
├── README.md                                   # 项目说明文档
└── src/main/java/com/heibaiying/
    ├── consumers/                              # 消费者示例
    │   ├── ConsumerASyn.java                   # 异步提交消费者
    │   ├── ConsumerSyn.java                    # 同步提交消费者
    │   ├── ConsumerASynAndSyn.java             # 混合提交策略消费者
    │   ├── ConsumerASynWithOffsets.java        # 手动偏移量管理消费者
    │   ├── ConsumerExit.java                   # 优雅退出消费者
    │   ├── ConsumerGroup.java                  # 消费者组示例
    │   ├── RebalanceListener.java              # 重平衡监听器消费者
    │   └── StandaloneConsumer.java             # 独立消费者
    └── producers/                              # 生产者示例
        ├── SimpleProducer.java                 # 简单生产者
        ├── ProducerASyn.java                   # 异步生产者
        ├── ProducerSyn.java                    # 同步生产者
        ├── ProducerWithPartitioner.java        # 使用自定义分区器的生产者
        └── partitioners/
            └── CustomPartitioner.java          # 自定义分区器实现
```

## 环境要求

- **JDK**: 8或更高版本
- **Maven**: 3.6或更高版本
- **Kafka**: 2.2.0或兼容版本
- **操作系统**: Windows/Linux/macOS

## 快速开始

### 1. 启动Kafka服务

在运行示例之前，请确保Kafka服务正在运行：

```bash
# 启动Zookeeper（如果使用Kafka 2.8之前的版本）
bin/zookeeper-server-start.sh config/zookeeper.properties

# 启动Kafka服务器
bin/kafka-server-start.sh config/server.properties
```

### 2. 创建Topic

运行示例前需要创建相应的Topic：

```bash
# 创建Hello-Kafka主题（用于基础示例）
kafka-topics.sh --create --topic Hello-Kafka --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 创建Kafka-Partitioner-Test主题（用于分区器示例）
kafka-topics.sh --create --topic Kafka-Partitioner-Test --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

### 3. 配置服务器地址

根据你的Kafka服务器地址，修改代码中的`bootstrap.servers`配置：

```java
// 默认配置（需要根据实际情况修改）
props.put("bootstrap.servers", "hadoop001:9092");

// 如果使用本地Kafka，修改为：
props.put("bootstrap.servers", "localhost:9092");
```

### 4. 编译项目

```bash
mvn clean compile
```

### 5. 运行示例

使用IDE运行各个示例类的main方法，或使用Maven命令：

```bash
# 运行简单生产者
mvn exec:java -Dexec.mainClass="com.heibaiying.producers.SimpleProducer"

# 运行异步消费者
mvn exec:java -Dexec.mainClass="com.heibaiying.consumers.ConsumerASyn"
```

## 示例说明

### 生产者示例

#### 1. SimpleProducer - 简单生产者
- **文件**: `src/main/java/com/heibaiying/producers/SimpleProducer.java`
- **功能**: 演示最基本的消息发送
- **知识点**: 
  - Kafka Producer基本配置
  - 序列化器的使用
  - 同步发送消息
  - 资源释放

#### 2. ProducerSyn - 同步生产者
- **文件**: `src/main/java/com/heibaiying/producers/ProducerSyn.java`
- **功能**: 演示同步发送消息并获取发送结果
- **知识点**:
  - 同步发送vs异步发送的区别
  - 同步发送的可靠性保证
  - RecordMetadata元数据获取
  - 适用于对可靠性要求高的场景

#### 3. ProducerASyn - 异步生产者
- **文件**: `src/main/java/com/heibaiying/producers/ProducerASyn.java`
- **功能**: 演示异步发送消息和回调处理
- **知识点**:
  - 异步发送的优势
  - 回调函数的使用
  - 异常处理机制
  - RecordMetadata的作用

#### 4. ProducerWithPartitioner - 自定义分区器生产者
- **文件**: `src/main/java/com/heibaiying/producers/ProducerWithPartitioner.java`
- **功能**: 演示如何使用自定义分区器
- **知识点**:
  - 自定义分区器的配置
  - 分区策略的影响
  - 参数传递机制

#### 5. CustomPartitioner - 自定义分区器实现
- **文件**: `src/main/java/com/heibaiying/producers/partitioners/CustomPartitioner.java`
- **功能**: 自定义分区逻辑的实现
- **知识点**:
  - Partitioner接口的实现
  - 分区算法设计
  - 配置参数的获取

### 消费者示例

#### 1. ConsumerSyn - 同步提交消费者
- **文件**: `src/main/java/com/heibaiying/consumers/ConsumerSyn.java`
- **功能**: 演示同步提交偏移量的消费者
- **知识点**:
  - 同步提交的可靠性
  - 阻塞特性和性能影响
  - 异常处理
  - 偏移量管理

#### 2. ConsumerASyn - 异步提交消费者
- **文件**: `src/main/java/com/heibaiying/consumers/ConsumerASyn.java`
- **功能**: 演示异步提交偏移量的消费者
- **知识点**:
  - 异步提交的性能优势
  - 回调处理机制
  - 提交失败的处理
  - 消费者组的概念

#### 3. ConsumerASynAndSyn - 混合提交策略消费者
- **文件**: `src/main/java/com/heibaiying/consumers/ConsumerASynAndSyn.java`
- **功能**: 演示结合异步和同步提交的消费者
- **知识点**:
  - 混合提交策略的优势
  - 异步提交提高性能，同步提交保证可靠性
  - 消费者关闭前的最终同步提交
  - 适用于高性能且需要可靠性保证的场景

#### 4. ConsumerASynWithOffsets - 手动偏移量管理消费者
- **文件**: `src/main/java/com/heibaiying/consumers/ConsumerASynWithOffsets.java`
- **功能**: 演示手动管理特定偏移量的消费者
- **知识点**:
  - 手动偏移量管理的精确控制
  - TopicPartition和OffsetAndMetadata的使用
  - 异步提交特定偏移量
  - 适用于需要精确控制消费进度的场景

#### 5. ConsumerExit - 优雅退出消费者
- **文件**: `src/main/java/com/heibaiying/consumers/ConsumerExit.java`
- **功能**: 演示消费者的优雅退出机制
- **知识点**:
  - wakeup()方法的使用
  - WakeupException异常处理
  - 多线程协作模式
  - 确保资源正确释放

#### 6. ConsumerGroup - 消费者组示例
- **文件**: `src/main/java/com/heibaiying/consumers/ConsumerGroup.java`
- **功能**: 演示消费者组的基本使用
- **知识点**:
  - 消费者组的核心概念
  - 分区自动分配机制
  - 自动提交偏移量
  - 负载均衡和故障转移

#### 7. RebalanceListener - 重平衡监听器消费者
- **文件**: `src/main/java/com/heibaiying/consumers/RebalanceListener.java`
- **功能**: 演示重平衡事件的监听和处理
- **知识点**:
  - ConsumerRebalanceListener接口
  - 重平衡前后的回调处理
  - 偏移量的手动管理
  - 确保消息不丢失和不重复

#### 8. StandaloneConsumer - 独立消费者
- **文件**: `src/main/java/com/heibaiying/consumers/StandaloneConsumer.java`
- **功能**: 演示不加入消费者组的独立消费者
- **知识点**:
  - 独立消费者vs消费者组的区别
  - 手动分区分配(assign vs subscribe)
  - 精确控制消费的分区
  - 适用于简单的单实例消费场景

## 核心概念解释

### 1. Producer（生产者）
- **作用**: 向Kafka主题发送消息
- **关键配置**:
  - `bootstrap.servers`: Kafka集群地址
  - `key.serializer`: 键序列化器
  - `value.serializer`: 值序列化器
  - `partitioner.class`: 分区器类（可选）

### 2. Consumer（消费者）
- **作用**: 从Kafka主题消费消息
- **关键配置**:
  - `bootstrap.servers`: Kafka集群地址
  - `group.id`: 消费者组ID
  - `key.deserializer`: 键反序列化器
  - `value.deserializer`: 值反序列化器
  - `enable.auto.commit`: 是否自动提交偏移量

### 3. Topic（主题）
- **定义**: 消息的分类，类似于数据库中的表
- **特点**: 可以有多个分区，支持并行处理

### 4. Partition（分区）
- **作用**: 实现Topic的水平扩展和并行处理
- **特点**: 每个分区内消息有序，分区间无序

### 5. Offset（偏移量）
- **定义**: 消息在分区中的唯一标识
- **作用**: 记录消费进度，支持重复消费

### 6. Consumer Group（消费者组）
- **作用**: 实现消费者的负载均衡
- **特点**: 同组内消费者不会重复消费同一消息

## 注意事项

### 1. 服务器配置
- 确保Kafka服务器正在运行
- 根据实际环境修改`bootstrap.servers`配置
- 确保网络连接正常

### 2. Topic管理
- 运行示例前先创建相应的Topic
- 合理设置分区数和副本数
- 注意Topic命名规范

### 3. 运行顺序
- 建议先运行生产者发送消息
- 再运行消费者接收消息
- 观察控制台输出结果

### 4. 资源管理
- 及时关闭Producer和Consumer
- 避免资源泄露
- 合理设置超时时间

### 5. 异常处理
- 注意网络异常的处理
- 关注序列化/反序列化异常
- 处理偏移量提交失败的情况

## 常见问题

### Q1: 连接Kafka失败
**A**: 检查以下几点：
- Kafka服务是否正常运行
- `bootstrap.servers`配置是否正确
- 网络连接是否正常
- 防火墙设置是否阻止连接

### Q2: Topic不存在错误
**A**: 确保已创建相应的Topic，或在Kafka配置中启用自动创建Topic功能。

### Q3: 消费者收不到消息
**A**: 检查以下几点：
- 生产者是否成功发送消息
- 消费者组ID是否正确
- 是否从正确的偏移量开始消费
- Topic和分区配置是否正确

### Q4: 序列化错误
**A**: 确保生产者和消费者使用相同的序列化/反序列化器配置。

## 扩展学习

完成基础示例后，建议继续学习：

1. **高级特性**:
   - 事务支持
   - 幂等性配置
   - 消息压缩
   - 安全认证

2. **性能优化**:
   - 批量发送配置
   - 缓冲区调优
   - 网络参数优化
   - 监控和指标

3. **集群管理**:
   - 多节点部署
   - 副本配置
   - 故障恢复
   - 数据备份

4. **生态系统**:
   - Kafka Connect
   - Kafka Streams
   - Schema Registry
   - 监控工具

## 参考资料

- [Apache Kafka官方文档](https://kafka.apache.org/documentation/)
- [Kafka Java客户端API文档](https://kafka.apache.org/28/javadoc/)
- [Kafka最佳实践指南](https://kafka.apache.org/documentation/#bestpractices)

---

**项目作者**: heibaiying  
**最后更新**: 2024年1月  
**许可证**: Apache License 2.0