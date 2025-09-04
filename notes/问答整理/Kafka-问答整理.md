# Kafka 问答整理

## Kafka异步发送重试机制

### 问题：在Kafka异步发送消息的示例中，如果在回调函数中抛出异常，并在外部try-catch捕获异常后执行i++，这种情况下是否能正常重试？

### 核心问题分析

用户提出的重试机制：
```java
for (int i = 0; i < maxRetries; i++) {
    try {
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    throw new RuntimeException(exception); // 在回调中抛出异常
                }
            }
        });
        break; // 发送成功，跳出循环
    } catch (Exception e) {
        // 外部try-catch捕获异常并重试
        System.out.println("发送失败，重试第 " + (i + 1) + " 次");
    }
}
```

### 答案：不能正常工作

**原因分析：**

1. **线程隔离问题**：异步发送时，回调函数在Kafka客户端的IO线程中执行，而外层的try-catch在主线程中，两者属于不同的线程。

2. **异常无法跨线程传播**：回调函数中抛出的异常无法被外层的try-catch捕获，因为异常不能跨线程传播。

3. **执行时序问题**：producer.send()方法立即返回，外层代码会立即执行break语句跳出循环，而此时回调函数可能还没有执行。

### 正确的重试机制实现

#### 1. 同步发送 + 手动重试
```java
for (int i = 0; i < maxRetries; i++) {
    try {
        // 使用get()方法将异步转为同步
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(); // 阻塞等待结果
        System.out.println("发送成功: " + metadata);
        break; // 成功后跳出循环
    } catch (Exception e) {
        System.out.println("发送失败，重试第 " + (i + 1) + " 次: " + e.getMessage());
        if (i == maxRetries - 1) {
            System.out.println("达到最大重试次数，发送失败");
        }
    }
}
```

#### 2. 异步发送 + 回调内重试
```java
public void sendWithRetry(ProducerRecord<String, String> record, int maxRetries) {
    sendWithRetry(record, maxRetries, 0);
}

private void sendWithRetry(ProducerRecord<String, String> record, int maxRetries, int currentAttempt) {
    producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                if (currentAttempt < maxRetries - 1) {
                    // 在回调函数内部进行重试
                    System.out.println("发送失败，重试第 " + (currentAttempt + 1) + " 次");
                    sendWithRetry(record, maxRetries, currentAttempt + 1);
                } else {
                    System.out.println("达到最大重试次数，发送失败: " + exception.getMessage());
                }
            } else {
                System.out.println("发送成功: " + metadata);
            }
        }
    });
}
```

#### 3. 使用CompletableFuture的重试
```java
public CompletableFuture<RecordMetadata> sendWithRetryAsync(ProducerRecord<String, String> record, int maxRetries) {
    return sendWithRetryAsync(record, maxRetries, 0);
}

private CompletableFuture<RecordMetadata> sendWithRetryAsync(ProducerRecord<String, String> record, int maxRetries, int currentAttempt) {
    CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
    
    producer.send(record, (metadata, exception) -> {
        if (exception != null) {
            if (currentAttempt < maxRetries - 1) {
                // 使用CompletableFuture链式处理重试
                sendWithRetryAsync(record, maxRetries, currentAttempt + 1)
                    .whenComplete((retryMetadata, retryException) -> {
                        if (retryException != null) {
                            future.completeExceptionally(retryException);
                        } else {
                            future.complete(retryMetadata);
                        }
                    });
            } else {
                future.completeExceptionally(exception);
            }
        } else {
            future.complete(metadata);
        }
    });
    
    return future;
}
```

### Kafka内置重试机制

**Kafka生产者提供了内置的重试机制**，通过以下配置参数控制：

```java
Properties props = new Properties();
// 设置重试次数
props.put(ProducerConfig.RETRIES_CONFIG, 3);
// 设置重试间隔
props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
// 设置可重试异常
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

**可重试的异常类型**：
   - RetriableException的子类
   - 网络异常（如连接超时）
   - 临时性错误（如分区leader选举中）
   - 资源不足异常

**不可重试的异常类型**：
   - 序列化异常
   - 消息过大异常
   - 认证失败异常
   - 配置错误异常

### 开发中常犯的错误

1. **误解异步执行模型**：认为异步回调中的异常可以被外层try-catch捕获
2. **混淆同步和异步重试逻辑**：在异步发送中使用同步重试的思维
3. **忽略Kafka内置重试**：自己实现重试而不利用Kafka的内置机制
4. **不区分可重试和不可重试异常**：对所有异常都进行重试
5. **没有设置合理的重试策略**：如指数退避、最大重试次数等

### 最佳实践建议

1. **优先使用Kafka内置重试**：配置合适的retries和retry.backoff.ms参数
2. **实现指数退避策略**：避免重试风暴，逐渐增加重试间隔
3. **区分异常类型**：只对可重试异常进行重试
4. **设置合理的超时时间**：避免无限等待
5. **监控和日志**：记录重试次数和失败原因，便于问题排查
6. **考虑业务影响**：根据业务重要性设置不同的重试策略

### 总结

用户提出的重试机制由于**异步执行的线程隔离特性**无法正常工作。正确的做法是：
- 使用同步发送进行重试
- 在异步回调内部实现重试逻辑
- 利用CompletableFuture处理异步结果
- 充分利用Kafka内置的重试机制

**核心要点**：异步编程中，异常处理和重试逻辑必须在同一个线程上下文中进行，不能跨线程传播异常。

---

## **Kafka生产者重复消息问题**

### 问题：Kafka生产者是否会发送重复消息？

### 答案：会发送重复消息

**重复消息产生的原因：**

1. **网络重试导致重复**：当网络出现问题时，生产者会重试发送消息，但如果第一次发送实际上已经成功，只是响应丢失，就会产生重复消息

2. **Broker故障恢复**：Broker在故障恢复过程中，可能会重复处理已经接收但未确认的消息

3. **生产者配置问题**：retries > 0 且 enable.idempotence = false 时容易产生重复

**重复消息的场景示例：**

<details>
<summary>点击展开代码示例</summary>

```java
// 容易产生重复消息的配置
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.RETRIES_CONFIG, 3); // 开启重试
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // 未开启幂等性

KafkaProducer<String, String> producer = new KafkaProducer<>(props);

// 网络异常时可能产生重复消息
ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "key1", "message1");
producer.send(record); // 可能因为网络问题重复发送
```

</details>

### **解决方案**

#### 1. **启用生产者幂等性（推荐）**

<details>
<summary>点击展开代码示例</summary>

```java
/**
 * 幂等性生产者配置类
 * 通过启用幂等性来避免重复消息
 */
public class IdempotentProducerConfig {
    
    /**
     * 创建幂等性生产者
     * @return 配置了幂等性的Kafka生产者
     */
    public static KafkaProducer<String, String> createIdempotentProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 启用幂等性（关键配置）
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // 幂等性要求的配置
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 必须设置为all
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // 重试次数
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // 最大未确认请求数
        
        return new KafkaProducer<>(props);
    }
}
```

</details>
#### 2. **使用事务（最强保证）**

<details>
<summary>点击展开代码示例</summary>

```java
/**
 * 事务性生产者类
 * 提供最强的消息传递保证
 */
public class TransactionalProducer {
    private KafkaProducer<String, String> producer;
    
    /**
     * 初始化事务性生产者
     */
    public TransactionalProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 事务配置
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        this.producer = new KafkaProducer<>(props);
        producer.initTransactions(); // 初始化事务
    }
    
    /**
     * 事务性发送消息
     * @param topic 主题
     * @param key 消息键
     * @param value 消息值
     */
    public void sendTransactionally(String topic, String key, String value) {
        try {
            producer.beginTransaction(); // 开始事务
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record);
            
            producer.commitTransaction(); // 提交事务
        } catch (Exception e) {
            producer.abortTransaction(); // 回滚事务
            throw new RuntimeException("Transaction failed", e);
        }
    }
}
```

</details>

#### 3. **应用层去重**

<details>
<summary>点击展开代码示例</summary>

```java
/**
 * 应用层去重生产者
 * 通过消息ID实现去重
 */
public class DeduplicatedProducer {
    private KafkaProducer<String, String> producer;
    private Set<String> sentMessageIds = new ConcurrentHashMap<String, Boolean>().keySet(ConcurrentHashMap.newKeySet());
    
    /**
     * 发送带唯一ID的消息
     * @param topic 主题
     * @param messageId 消息唯一ID
     * @param content 消息内容
     */
    public void sendWithDeduplication(String topic, String messageId, String content) {
        // 检查是否已发送
        if (sentMessageIds.contains(messageId)) {
            System.out.println("消息已发送，跳过: " + messageId);
            return;
        }
        
        // 构造包含ID的消息
        String messageWithId = messageId + "|" + content;
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageId, messageWithId);
        
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                sentMessageIds.add(messageId); // 记录已发送的消息ID
                System.out.println("消息发送成功: " + messageId);
            } else {
                System.err.println("消息发送失败: " + messageId + ", 错误: " + exception.getMessage());
            }
        });
    }
}
```

</details>

#### 4. **消费者端去重**

<details>
<summary>点击展开代码示例</summary>

```java
/**
 * 消费者端去重处理
 * 通过维护已处理消息集合实现去重
 */
public class DeduplicatedConsumer {
    private Set<String> processedMessageIds = new ConcurrentHashMap<String, Boolean>().keySet(ConcurrentHashMap.newKeySet());
    
    /**
     * 处理消息并去重
     * @param record 消费到的消息记录
     */
    public void processMessage(ConsumerRecord<String, String> record) {
        String messageId = extractMessageId(record.value());
        
        // 检查是否已处理
        if (processedMessageIds.contains(messageId)) {
            System.out.println("消息已处理，跳过: " + messageId);
            return;
        }
        
        try {
            // 业务逻辑处理
            processBusinessLogic(record);
            
            // 标记为已处理
            processedMessageIds.add(messageId);
            System.out.println("消息处理完成: " + messageId);
        } catch (Exception e) {
            System.err.println("消息处理失败: " + messageId + ", 错误: " + e.getMessage());
        }
    }
    
    /**
     * 从消息中提取唯一ID
     */
    private String extractMessageId(String message) {
        return message.split("\\|")[0];
    }
    
    /**
     * 业务逻辑处理
     */
    private void processBusinessLogic(ConsumerRecord<String, String> record) {
        // 实际的业务处理逻辑
        System.out.println("处理消息: " + record.value());
    }
}
```

</details>

### **幂等性 vs 事务对比**

| 特性 | 幂等性 | 事务 |
|------|--------|------|
| **作用范围** | 单个分区内 | 跨分区、跨主题 |
| **性能影响** | 较小 | 较大 |
| **实现复杂度** | 简单 | 复杂 |
| **一致性保证** | 分区级别 | 全局级别 |
| **适用场景** | 单分区高吞吐 | 多分区强一致性 |

### **开发中常犯的错误**

1. **只配置重试不开启幂等性**：设置retries > 0但enable.idempotence = false
2. **误解幂等性作用范围**：认为幂等性可以跨分区工作
3. **事务配置不当**：未正确设置transactional.id或未调用initTransactions()
4. **忽略消费者端去重**：只在生产者端处理重复，忽略消费者端的幂等性处理
5. **性能与一致性权衡不当**：在不需要强一致性的场景使用事务

### **最佳实践建议**

1. **默认启用幂等性**：在生产环境中默认设置enable.idempotence=true
2. **根据业务需求选择方案**：高吞吐场景用幂等性，强一致性场景用事务
3. **实现端到端去重**：生产者防重复 + 消费者幂等处理
4. **监控重复消息**：通过指标监控重复消息的产生和处理情况
5. **合理设置重试参数**：避免过度重试导致的性能问题

---

## **Kafka生产者幂等性机制**

### 问题：为什么Kafka生产者只能保证单个分区内的幂等性？

### **核心原理分析**

#### 1. **基于PID和序列号机制**

Kafka幂等性通过Producer ID (PID) + 分区 + 序列号的组合来实现去重：

   - **PID**：每个生产者实例分配唯一的Producer ID
   - **序列号**：每个分区维护独立的序列号，从0开始递增
   - **去重逻辑**：Broker检查(PID, 分区, 序列号)三元组是否重复

<details>
<summary>点击展开幂等性工作原理图示</summary>

```text
生产者端：
PID: 12345
分区0: 序列号 0, 1, 2, 3...
分区1: 序列号 0, 1, 2, 3...
分区2: 序列号 0, 1, 2, 3...

Broker端状态维护：
分区0: (PID=12345, Seq=3) ✓ 最新序列号
分区1: (PID=12345, Seq=2) ✓ 最新序列号  
分区2: (PID=12345, Seq=1) ✓ 最新序列号
```

</details>

#### 2. **分区级别的序列号隔离**

<details>
<summary>点击展开代码示例</summary>

```java
/**
 * 演示分区级别序列号隔离
 */
public class PartitionSequenceDemo {
    
    /**
     * 演示不同分区的序列号独立性
     */
    public static void demonstratePartitionIsolation() {
        Properties props = new Properties();
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 其他配置...
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // 同一个生产者，不同分区的序列号独立计算
        // 分区0: 序列号从0开始
        producer.send(new ProducerRecord<>("topic", 0, "key1", "msg1")); // Seq: 0
        producer.send(new ProducerRecord<>("topic", 0, "key2", "msg2")); // Seq: 1
        
        // 分区1: 序列号也从0开始（独立计算）
        producer.send(new ProducerRecord<>("topic", 1, "key3", "msg3")); // Seq: 0
        producer.send(new ProducerRecord<>("topic", 1, "key4", "msg4")); // Seq: 1
        
        // 每个分区维护自己的序列号状态，互不影响
    }
}
```

</details>

#### 3. **Broker端状态维护**

Broker为每个分区维护一个映射表：(PID → 最新序列号)

<details>
<summary>点击展开Broker端逻辑示例</summary>

```java
/**
 * Broker端幂等性检查逻辑（简化版）
 */
public class BrokerIdempotenceChecker {
    
    // 每个分区维护独立的状态映射
    private Map<Integer, Map<Long, Long>> partitionProducerSequence = new HashMap<>();
    
    /**
     * 检查消息是否重复
     * @param partition 分区号
     * @param producerId 生产者ID
     * @param sequence 序列号
     * @return true表示重复消息
     */
    public boolean isDuplicate(int partition, long producerId, long sequence) {
        Map<Long, Long> producerSeqMap = partitionProducerSequence.computeIfAbsent(partition, k -> new HashMap<>());
        
        Long lastSequence = producerSeqMap.get(producerId);
        
        if (lastSequence == null) {
            // 首次发送
            producerSeqMap.put(producerId, sequence);
            return false;
        }
        
        if (sequence <= lastSequence) {
            // 序列号小于等于已记录的最大序列号，判定为重复
            return true;
        }
        
        // 更新最新序列号
        producerSeqMap.put(producerId, sequence);
        return false;
    }
}
```

</details>

### **为什么不能跨分区？**

#### 1. **架构设计限制**

   - **分区独立性**：Kafka的分区设计本身就是为了实现水平扩展和并行处理，每个分区独立管理
   - **性能考虑**：跨分区协调会引入额外的同步开销，影响高吞吐量性能
   - **一致性复杂度**：跨分区幂等性需要分布式事务协调，实现复杂度高

#### 2. **状态维护成本**

<details>
<summary>点击展开跨分区状态维护复杂性分析</summary>

```java
/**
 * 跨分区幂等性的复杂性演示
 */
public class CrossPartitionComplexity {
    
    /**
     * 如果要实现跨分区幂等性，需要的额外机制
     */
    public class CrossPartitionIdempotence {
        // 需要全局状态协调
        private Map<Long, GlobalSequenceState> globalProducerState;
        
        // 需要分布式锁或协调机制
        private DistributedLock globalLock;
        
        // 需要跨分区通信
        private CrossPartitionCommunicator communicator;
        
        /**
         * 跨分区幂等性检查（伪代码）
         * 复杂度和性能开销都很高
         */
        public boolean checkCrossPartitionDuplicate(long producerId, long globalSequence) {
            // 1. 获取全局锁
            globalLock.lock();
            try {
                // 2. 查询所有相关分区的状态
                GlobalSequenceState state = globalProducerState.get(producerId);
                
                // 3. 跨分区状态同步和检查
                boolean isDuplicate = communicator.checkAllPartitions(producerId, globalSequence);
                
                // 4. 更新全局状态
                if (!isDuplicate) {
                    updateGlobalState(producerId, globalSequence);
                }
                
                return isDuplicate;
            } finally {
                // 5. 释放全局锁
                globalLock.unlock();
            }
        }
    }
}
```

</details>

### **解决跨分区重复消息的方案**

#### 1. **使用事务**

<details>
<summary>点击展开事务解决方案</summary>

```java
/**
 * 使用事务解决跨分区重复消息
 */
public class CrossPartitionTransactionalSolution {
    private KafkaProducer<String, String> producer;
    
    public CrossPartitionTransactionalSolution() {
        Properties props = new Properties();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "cross-partition-tx");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        this.producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }
    
    /**
     * 跨分区事务性发送
     * 保证多个分区的消息要么全部成功，要么全部失败
     */
    public void sendCrossPartitionTransactionally(List<ProducerRecord<String, String>> records) {
        try {
            producer.beginTransaction();
            
            // 在同一个事务中发送到多个分区
            for (ProducerRecord<String, String> record : records) {
                producer.send(record);
            }
            
            producer.commitTransaction();
            System.out.println("跨分区事务提交成功");
        } catch (Exception e) {
            producer.abortTransaction();
            System.err.println("跨分区事务回滚: " + e.getMessage());
            throw e;
        }
    }
}
```

</details>

#### 2. **应用层唯一标识**

<details>
<summary>点击展开应用层解决方案</summary>

```java
/**
 * 应用层跨分区去重方案
 */
public class ApplicationLevelDeduplication {
    
    /**
     * 生成全局唯一消息ID
     * @return 全局唯一标识
     */
    public String generateGlobalMessageId() {
        // 方案1: UUID
        return UUID.randomUUID().toString();
        
        // 方案2: 时间戳 + 机器ID + 序列号
        // return System.currentTimeMillis() + "-" + machineId + "-" + sequence.incrementAndGet();
        
        // 方案3: 雪花算法
        // return snowflakeIdGenerator.nextId();
    }
    
    /**
     * 发送带全局ID的消息到多个分区
     */
    public void sendWithGlobalId(String topic, String content) {
        String globalId = generateGlobalMessageId();
        
        // 构造包含全局ID的消息
        String messageWithId = globalId + "|" + content;
        
        // 发送到多个分区（使用相同的全局ID）
        for (int partition = 0; partition < 3; partition++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, globalId, messageWithId);
            producer.send(record);
        }
        
        System.out.println("发送跨分区消息，全局ID: " + globalId);
    }
}
```

</details>

#### 3. **固定分区策略**

<details>
<summary>点击展开固定分区策略</summary>

```java
/**
 * 固定分区策略避免跨分区问题
 */
public class FixedPartitionStrategy {
    
    /**
     * 自定义分区器，确保相关消息发送到同一分区
     */
    public static class BusinessKeyPartitioner implements Partitioner {
        
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, 
                           Object value, byte[] valueBytes, Cluster cluster) {
            
            if (key == null) {
                throw new IllegalArgumentException("Key cannot be null for business partitioning");
            }
            
            // 根据业务键计算分区，确保相关消息在同一分区
            String businessKey = extractBusinessKey(key.toString());
            int partitionCount = cluster.partitionCountForTopic(topic);
            
            return Math.abs(businessKey.hashCode()) % partitionCount;
        }
        
        /**
         * 提取业务相关的键
         */
        private String extractBusinessKey(String key) {
            // 例如：从 "user123_order456" 中提取 "user123"
            return key.split("_")[0];
        }
        
        @Override
        public void close() {}
        
        @Override
        public void configure(Map<String, ?> configs) {}
    }
    
    /**
     * 使用固定分区策略的生产者配置
     */
    public static KafkaProducer<String, String> createFixedPartitionProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 启用幂等性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // 使用自定义分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, BusinessKeyPartitioner.class.getName());
        
        return new KafkaProducer<>(props);
    }
}
```

</details>

### **开发中常见错误**

1. **误解幂等性范围**：认为enable.idempotence=true可以解决所有重复消息问题
2. **跨分区发送相同内容**：在多个分区发送相同消息而不做去重处理
3. **忽略分区策略**：没有根据业务需求选择合适的分区策略
4. **过度依赖幂等性**：在需要跨分区一致性的场景仍然只使用幂等性

### **最佳实践**

1. **理解幂等性作用范围**：幂等性只在单分区内有效，跨分区需要其他方案
2. **根据业务选择方案**：单分区用幂等性，跨分区用事务或应用层去重
3. **合理设计分区策略**：让相关的消息尽量发送到同一分区
4. **实现端到端去重**：生产者端防重复 + 消费者端幂等处理
5. **监控和告警**：监控重复消息的产生和处理情况

---

## **Kafka避免重复消息的设计方案和最佳实践**

### 问题：在设计上如何避免Kafka生产者和消费者产生重复消息，推荐的实现和最佳实践？

### **生产者端防重复设计**

#### 1. **启用幂等性配置**

<details>
<summary>点击展开生产者幂等性配置</summary>

```java
/**
 * 生产者端防重复配置类
 * 提供完整的幂等性和事务配置方案
 */
public class ProducerAntiDuplicationConfig {
    
    /**
     * 创建幂等性生产者（推荐用于单分区场景）
     * @return 配置了幂等性的生产者
     */
    public static KafkaProducer<String, String> createIdempotentProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 核心幂等性配置
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 必须为all
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // 性能优化配置
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return new KafkaProducer<>(props);
    }
    
    /**
     * 创建事务性生产者（推荐用于跨分区场景）
     * @param transactionalId 事务ID
     * @return 配置了事务的生产者
     */
    public static KafkaProducer<String, String> createTransactionalProducer(String transactionalId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 事务配置
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // 事务性能配置
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        return new KafkaProducer<>(props);
    }
}
```

</details>

#### 2. **应用层唯一标识设计**

<details>
<summary>点击展开应用层唯一标识方案</summary>

```java
/**
 * 应用层消息唯一标识生成器
 * 提供多种唯一ID生成策略
 */
public class MessageIdGenerator {
    private static final AtomicLong sequence = new AtomicLong(0);
    private static final String MACHINE_ID = getMachineId();
    
    /**
     * 生成基于时间戳的唯一ID
     * 格式：timestamp-machineId-sequence
     * @return 唯一消息ID
     */
    public static String generateTimeBasedId() {
        long timestamp = System.currentTimeMillis();
        long seq = sequence.incrementAndGet();
        return timestamp + "-" + MACHINE_ID + "-" + seq;
    }
    
    /**
     * 生成基于业务的唯一ID
     * @param businessType 业务类型
     * @param businessId 业务ID
     * @return 业务唯一ID
     */
    public static String generateBusinessId(String businessType, String businessId) {
        return businessType + "-" + businessId + "-" + System.currentTimeMillis();
    }
    
    /**
     * 生成UUID
     * @return UUID字符串
     */
    public static String generateUUID() {
        return UUID.randomUUID().toString();
    }
    
    /**
     * 获取机器ID（简化实现）
     */
    private static String getMachineId() {
        try {
            return InetAddress.getLocalHost().getHostAddress().replaceAll("\\.", "");
        } catch (Exception e) {
            return "unknown";
        }
    }
}

/**
 * 带唯一ID的消息发送器
 */
public class UniqueMessageSender {
    private final KafkaProducer<String, String> producer;
    private final Set<String> sentMessageIds = ConcurrentHashMap.newKeySet();
    
    public UniqueMessageSender(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }
    
    /**
     * 发送带唯一ID的消息
     * @param topic 主题
     * @param businessType 业务类型
     * @param businessId 业务ID
     * @param content 消息内容
     */
    public void sendUniqueMessage(String topic, String businessType, String businessId, String content) {
        String messageId = MessageIdGenerator.generateBusinessId(businessType, businessId);
        
        // 检查是否已发送
        if (sentMessageIds.contains(messageId)) {
            System.out.println("消息已发送，跳过重复发送: " + messageId);
            return;
        }
        
        // 构造包含ID的消息
        MessageWithId messageWithId = new MessageWithId(messageId, content, System.currentTimeMillis());
        String jsonMessage = JSON.toJSONString(messageWithId);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageId, jsonMessage);
        
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                sentMessageIds.add(messageId);
                System.out.println("消息发送成功: " + messageId);
            } else {
                System.err.println("消息发送失败: " + messageId + ", 错误: " + exception.getMessage());
            }
        });
    }
    
    /**
     * 消息结构体
     */
    public static class MessageWithId {
        private String id;
        private String content;
        private long timestamp;
        
        // 构造函数、getter、setter省略
    }
}
```

</details>

### **消费者端幂等处理设计**

#### 1. **手动提交偏移量**

<details>
<summary>点击展开手动提交偏移量方案</summary>

```java
/**
 * 手动提交偏移量的消费者
 * 确保消息处理完成后再提交偏移量
 */
public class ManualCommitConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();
    
    public ManualCommitConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 关闭自动提交，使用手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // 性能优化配置
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        
        this.consumer = new KafkaConsumer<>(props);
    }
    
    /**
     * 消费消息并手动提交偏移量
     * @param topics 订阅的主题列表
     */
    public void consumeWithManualCommit(List<String> topics) {
        consumer.subscribe(topics);
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // 幂等性处理消息
                        if (processMessageIdempotently(record)) {
                            System.out.println("消息处理成功: " + record.key());
                        } else {
                            System.out.println("消息已处理，跳过: " + record.key());
                        }
                    } catch (Exception e) {
                        System.err.println("消息处理失败: " + record.key() + ", 错误: " + e.getMessage());
                        // 根据业务需求决定是否继续处理后续消息
                        continue;
                    }
                }
                
                // 所有消息处理完成后，手动提交偏移量
                try {
                    consumer.commitSync();
                    System.out.println("偏移量提交成功");
                } catch (CommitFailedException e) {
                    System.err.println("偏移量提交失败: " + e.getMessage());
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    /**
     * 幂等性处理消息
     * @param record 消息记录
     * @return true表示消息被处理，false表示消息已处理过
     */
    private boolean processMessageIdempotently(ConsumerRecord<String, String> record) {
        String messageId = extractMessageId(record);
        
        // 检查消息是否已处理
        if (processedMessageIds.contains(messageId)) {
            return false;
        }
        
        // 执行业务逻辑
        processBusinessLogic(record);
        
        // 标记消息已处理
        processedMessageIds.add(messageId);
        return true;
    }
    
    /**
     * 提取消息ID
     */
    private String extractMessageId(ConsumerRecord<String, String> record) {
        // 方案1: 使用消息key作为ID
        if (record.key() != null) {
            return record.key();
        }
        
        // 方案2: 从消息内容中解析ID
        try {
            MessageWithId message = JSON.parseObject(record.value(), MessageWithId.class);
            return message.getId();
        } catch (Exception e) {
            // 方案3: 使用offset作为兜底ID
            return record.topic() + "-" + record.partition() + "-" + record.offset();
        }
    }
    
    /**
     * 业务逻辑处理
     */
    private void processBusinessLogic(ConsumerRecord<String, String> record) {
        // 实际的业务处理逻辑
        System.out.println("处理消息: " + record.value());
        
        // 模拟处理时间
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

</details>

#### 2. **数据库事务保证**

<details>
<summary>点击展开数据库事务方案</summary>

```java
/**
 * 基于数据库事务的消费者幂等性保证
 * 将消息处理和偏移量存储在同一个事务中
 */
@Service
public class TransactionalConsumerService {
    
    @Autowired
    private DataSource dataSource;
    
    @Autowired
    private BusinessService businessService;
    
    /**
     * 在数据库事务中处理消息
     * @param record 消息记录
     */
    @Transactional(rollbackFor = Exception.class)
    public void processMessageInTransaction(ConsumerRecord<String, String> record) {
        String messageId = extractMessageId(record);
        
        try {
            // 检查消息是否已处理（基于数据库）
            if (isMessageProcessed(messageId)) {
                System.out.println("消息已处理，跳过: " + messageId);
                return;
            }
            
            // 执行业务逻辑
            businessService.processMessage(record.value());
            
            // 记录消息已处理
            markMessageAsProcessed(messageId, record);
            
            // 存储偏移量信息
            storeOffsetInfo(record);
            
            System.out.println("消息处理完成: " + messageId);
            
        } catch (Exception e) {
            System.err.println("消息处理失败: " + messageId + ", 错误: " + e.getMessage());
            throw e; // 触发事务回滚
        }
    }
    
    /**
     * 检查消息是否已处理
     */
    private boolean isMessageProcessed(String messageId) {
        String sql = "SELECT COUNT(*) FROM processed_messages WHERE message_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, messageId);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("检查消息状态失败", e);
        }
        return false;
    }
    
    /**
     * 标记消息为已处理
     */
    private void markMessageAsProcessed(String messageId, ConsumerRecord<String, String> record) {
        String sql = "INSERT INTO processed_messages (message_id, topic, partition_id, offset_value, processed_time) VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, messageId);
            stmt.setString(2, record.topic());
            stmt.setInt(3, record.partition());
            stmt.setLong(4, record.offset());
            stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("标记消息失败", e);
        }
    }
    
    /**
     * 存储偏移量信息
     */
    private void storeOffsetInfo(ConsumerRecord<String, String> record) {
        String sql = "INSERT INTO consumer_offsets (topic, partition_id, offset_value, update_time) " +
                    "VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE offset_value = ?, update_time = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, record.topic());
            stmt.setInt(2, record.partition());
            stmt.setLong(3, record.offset() + 1); // 下一个要消费的偏移量
            stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            stmt.setLong(5, record.offset() + 1);
            stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("存储偏移量失败", e);
        }
    }
}
```

</details>

### **端到端幂等性设计**

#### 1. **分层防重设计**

<details>
<summary>点击展开分层防重架构</summary>

```java
/**
 * 分层防重架构设计
 * 在多个层面实现重复消息防护
 */
public class LayeredAntiDuplicationArchitecture {
    
    /**
     * 第一层：生产者端防重
     */
    public static class ProducerLayer {
        private final KafkaProducer<String, String> producer;
        private final RedisTemplate<String, String> redisTemplate;
        
        /**
         * 发送消息前检查是否已发送
         */
        public boolean sendIfNotExists(String messageId, String topic, String content) {
            String lockKey = "producer_lock:" + messageId;
            String sentKey = "sent_message:" + messageId;
            
            // 使用Redis分布式锁防止并发重复发送
            Boolean lockAcquired = redisTemplate.opsForValue().setIfAbsent(lockKey, "1", Duration.ofMinutes(5));
            if (!lockAcquired) {
                System.out.println("获取锁失败，可能正在发送: " + messageId);
                return false;
            }
            
            try {
                // 检查是否已发送
                if (redisTemplate.hasKey(sentKey)) {
                    System.out.println("消息已发送: " + messageId);
                    return false;
                }
                
                // 发送消息
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageId, content);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        // 标记消息已发送
                        redisTemplate.opsForValue().set(sentKey, "1", Duration.ofDays(1));
                        System.out.println("消息发送成功: " + messageId);
                    }
                });
                
                return true;
            } finally {
                redisTemplate.delete(lockKey);
            }
        }
    }
    
    /**
     * 第二层：消费者端防重
     */
    public static class ConsumerLayer {
        private final RedisTemplate<String, String> redisTemplate;
        
        /**
         * 幂等性处理消息
         */
        public boolean processIfNotProcessed(String messageId, Runnable businessLogic) {
            String processedKey = "processed_message:" + messageId;
            String lockKey = "consumer_lock:" + messageId;
            
            // 使用Redis分布式锁防止并发重复处理
            Boolean lockAcquired = redisTemplate.opsForValue().setIfAbsent(lockKey, "1", Duration.ofMinutes(5));
            if (!lockAcquired) {
                System.out.println("获取锁失败，可能正在处理: " + messageId);
                return false;
            }
            
            try {
                // 检查是否已处理
                if (redisTemplate.hasKey(processedKey)) {
                    System.out.println("消息已处理: " + messageId);
                    return false;
                }
                
                // 执行业务逻辑
                businessLogic.run();
                
                // 标记消息已处理
                redisTemplate.opsForValue().set(processedKey, "1", Duration.ofDays(7));
                System.out.println("消息处理完成: " + messageId);
                
                return true;
            } finally {
                redisTemplate.delete(lockKey);
            }
        }
    }
    
    /**
     * 第三层：存储层防重
     */
    public static class StorageLayer {
        
        /**
         * 数据库层面的唯一约束
         */
        @Entity
        @Table(name = "business_records", 
               uniqueConstraints = @UniqueConstraint(columnNames = {"message_id", "business_type"}))
        public static class BusinessRecord {
            @Id
            private String id;
            
            @Column(name = "message_id", nullable = false)
            private String messageId;
            
            @Column(name = "business_type", nullable = false)
            private String businessType;
            
            @Column(name = "content")
            private String content;
            
            @Column(name = "created_time")
            private LocalDateTime createdTime;
            
            // getter、setter省略
        }
        
        /**
         * 使用数据库唯一约束防重
         */
        public boolean insertIfNotExists(BusinessRecord record) {
            try {
                // 依赖数据库唯一约束防止重复插入
                businessRecordRepository.save(record);
                return true;
            } catch (DataIntegrityViolationException e) {
                System.out.println("记录已存在，跳过插入: " + record.getMessageId());
                return false;
            }
        }
    }
}
```

</details>

### **开发中常见错误**

1. **只在单一层面防重**：只在生产者或消费者一端处理重复，没有端到端设计
2. **忽略并发场景**：没有考虑多实例并发处理同一消息的情况
3. **状态存储不可靠**：使用内存存储已处理消息ID，重启后丢失状态
4. **没有设置过期时间**：防重状态永久存储，导致存储空间无限增长
5. **异常处理不当**：处理失败时没有正确清理防重状态

### **最佳实践建议**

1. **生产者端防止重复**：启用幂等性，使用唯一消息ID，实现发送前检查
2. **消费者端幂等处理**：手动提交偏移量，实现业务幂等性，使用分布式锁
3. **存储层状态一致性**：使用数据库事务，设置唯一约束，实现原子操作
4. **监控层问题发现**：监控重复消息指标，设置告警阈值，记录处理日志
5. **分层防重设计**：在多个层面实现防重机制，确保端到端的消息唯一性

---

## **Kafka消费者手动提交偏移量**

### 问题：Kafka消费者手动提交偏移量的概念及其原因？

### **手动提交偏移量概念**

手动提交偏移量是指消费者在处理完消息后，主动调用API提交当前消费位置，而不是依赖Kafka客户端的自动提交机制。

### **自动提交 vs 手动提交对比**

| 特性 | 自动提交 | 手动提交 |
|------|----------|----------|
| **控制粒度** | 粗粒度，按时间间隔 | 细粒度，按消息处理完成 |
| **可靠性** | 可能丢失或重复消息 | 确保消息处理完成后提交 |
| **实现复杂度** | 简单 | 相对复杂 |
| **性能影响** | 较小 | 略大（需要额外API调用） |
| **适用场景** | 对消息丢失不敏感 | 要求精确处理每条消息 |

### **为什么需要手动提交？**

#### 1. **确保消息处理可靠性**

<details>
<summary>点击展开可靠性问题分析</summary>

```java
/**
 * 自动提交可能导致的问题演示
 */
public class AutoCommitProblems {
    
    /**
     * 自动提交导致消息丢失的场景
     */
    public void demonstrateMessageLoss() {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // 自动提交
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); // 5秒提交一次
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // 假设这里处理消息需要10秒
                    processMessage(record); // 耗时操作
                    
                } catch (Exception e) {
                    // 如果在处理过程中发生异常，但偏移量可能已经自动提交
                    // 这会导致消息丢失，因为Kafka认为消息已经被处理
                    System.err.println("消息处理失败，但偏移量可能已提交: " + record.offset());
                }
            }
            
            // 在这个循环中，如果自动提交间隔到了，偏移量会被提交
            // 即使某些消息还在处理中或处理失败
        }
    }
    
    /**
     * 模拟耗时的消息处理
     */
    private void processMessage(ConsumerRecord<String, String> record) throws Exception {
        // 模拟复杂的业务处理
        Thread.sleep(10000); // 10秒处理时间
        
        // 模拟可能的处理失败
        if (Math.random() < 0.1) {
            throw new RuntimeException("处理失败");
        }
        
        System.out.println("消息处理成功: " + record.value());
    }
}
```

</details>

#### 2. **实现精确的消息处理控制**

<details>
<summary>点击展开精确控制方案</summary>

```java
/**
 * 手动提交实现精确的消息处理控制
 */
public class PreciseMessageControl {
    private final KafkaConsumer<String, String> consumer;
    
    public PreciseMessageControl() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "precise-control-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        this.consumer = new KafkaConsumer<>(props);
    }
    
    /**
     * 逐条消息处理并提交
     * 确保每条消息处理完成后再提交偏移量
     */
    public void processMessagesOneByOne() {
        consumer.subscribe(Arrays.asList("test-topic"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // 处理单条消息
                        processMessage(record);
                        
                        // 处理成功后立即提交当前消息的偏移量
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                        offsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                        );
                        
                        consumer.commitSync(offsets);
                        System.out.println("消息处理并提交成功: offset=" + record.offset());
                        
                    } catch (Exception e) {
                        System.err.println("消息处理失败，不提交偏移量: offset=" + record.offset() + ", 错误: " + e.getMessage());
                        
                        // 根据业务需求决定是否继续处理后续消息
                        // 选项1: 停止处理，等待人工干预
                        // break;
                        
                        // 选项2: 跳过当前消息，继续处理（可能导致消息丢失）
                        // continue;
                        
                        // 选项3: 重试处理
                        retryProcessMessage(record);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    /**
     * 批量处理并提交
     * 处理一批消息后统一提交偏移量
     */
    public void processBatchAndCommit() {
        consumer.subscribe(Arrays.asList("test-topic"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                boolean allProcessedSuccessfully = true;
                
                // 处理一批消息
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processMessage(record);
                    } catch (Exception e) {
                        System.err.println("消息处理失败: " + record.offset());
                        allProcessedSuccessfully = false;
                        break; // 有消息处理失败，停止处理当前批次
                    }
                }
                
                // 只有所有消息都处理成功才提交偏移量
                if (allProcessedSuccessfully && !records.isEmpty()) {
                    try {
                        consumer.commitSync();
                        System.out.println("批次处理完成，偏移量提交成功");
                    } catch (CommitFailedException e) {
                        System.err.println("偏移量提交失败: " + e.getMessage());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    /**
     * 异步提交偏移量
     * 提高性能，但需要处理提交失败的情况
     */
    public void processWithAsyncCommit() {
        consumer.subscribe(Arrays.asList("test-topic"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processMessage(record);
                    } catch (Exception e) {
                        System.err.println("消息处理失败: " + record.offset());
                        continue;
                    }
                }
                
                // 异步提交偏移量，不阻塞后续处理
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        System.err.println("异步提交偏移量失败: " + exception.getMessage());
                        // 可以在这里实现重试逻辑
                    } else {
                        System.out.println("异步提交偏移量成功");
                    }
                });
            }
        } finally {
            // 关闭前进行同步提交，确保最后的偏移量被提交
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }
    
    /**
     * 重试处理消息
     */
    private void retryProcessMessage(ConsumerRecord<String, String> record) {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Thread.sleep(1000 * (i + 1)); // 指数退避
                processMessage(record);
                
                // 重试成功，提交偏移量
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1)
                );
                consumer.commitSync(offsets);
                
                System.out.println("重试成功: offset=" + record.offset());
                return;
            } catch (Exception e) {
                System.err.println("重试失败 " + (i + 1) + "/" + maxRetries + ": " + e.getMessage());
            }
        }
        
        // 重试全部失败，记录到死信队列或告警
        handleFailedMessage(record);
    }
    
    /**
     * 处理失败的消息
     */
    private void handleFailedMessage(ConsumerRecord<String, String> record) {
        // 发送到死信队列或记录到数据库
        System.err.println("消息处理最终失败，需要人工处理: " + record.offset());
    }
    
    /**
     * 模拟消息处理
     */
    private void processMessage(ConsumerRecord<String, String> record) throws Exception {
        // 模拟业务处理
        System.out.println("处理消息: " + record.value());
        
        // 模拟可能的处理失败
        if (Math.random() < 0.1) {
            throw new RuntimeException("随机处理失败");
        }
    }
}
```

</details>

### **手动提交的两种方式**

#### 1. **同步提交（commitSync）**

**特点**：
   - 阻塞等待提交完成
   - 可以确保提交成功
   - 性能相对较低
   - 适合对一致性要求高的场景

#### 2. **异步提交（commitAsync）**

**特点**：
   - 非阻塞，不等待提交完成
   - 性能较高
   - 需要处理提交失败的情况
   - 适合对性能要求高的场景

### **开发中常犯的错误**

1. **提交时机错误**：在消息处理前就提交偏移量
2. **异常处理不当**：处理失败时仍然提交偏移量
3. **忽略提交失败**：使用异步提交但不处理失败回调
4. **重复提交**：对同一偏移量多次提交
5. **没有最终同步提交**：程序退出前没有进行最后的同步提交

### **最佳实践**

1. **根据业务需求选择提交方式**：高一致性用同步提交，高性能用异步提交
2. **确保处理完成后再提交**：只有消息成功处理后才提交偏移量
3. **实现合适的异常处理**：处理失败时不提交，或实现重试机制
4. **监控提交状态**：监控提交成功率和失败情况
5. **程序退出时同步提交**：确保最后的偏移量被正确提交

### **核心价值**
 
 手动提交偏移量的核心价值在于确保消息处理的可靠性和精确性，避免消息丢失和重复处理，实现真正的至少一次（At Least Once）或精确一次（Exactly Once）语义。

---

## **Kafka消息丢失的各种场景**

### 问题：除了自动提交外，Kafka消息丢失还有哪些场景？

### **生产者端消息丢失场景**

#### 1. **异步发送未等待确认**

<details>
<summary>点击展开异步发送问题分析</summary>

```java
/**
 * 异步发送导致消息丢失的危险场景
 */
public class ProducerMessageLossScenarios {
    
    /**
     * 危险场景1：异步发送后立即退出程序
     */
    public void dangerousAsyncSend() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // 危险：异步发送后立即关闭生产者
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("test-topic", "message-" + i));
        }
        
        // 这里没有等待发送完成就关闭，可能导致消息丢失
        producer.close(); // 危险操作
    }
    
    /**
     * 安全场景：等待所有消息发送完成
     */
    public void safeAsyncSend() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        
        // 收集所有发送的Future
        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>("test-topic", "message-" + i)
            );
            futures.add(future);
        }
        
        // 等待所有消息发送完成
        for (Future<RecordMetadata> future : futures) {
            try {
                RecordMetadata metadata = future.get(10, TimeUnit.SECONDS);
                System.out.println("消息发送成功: offset=" + metadata.offset());
            } catch (Exception e) {
                System.err.println("消息发送失败: " + e.getMessage());
            }
        }
        
        producer.close();
    }
}
```

</details>

#### 2. **acks配置不当**

<details>
<summary>点击展开acks配置分析</summary>

```java
/**
 * acks配置对消息可靠性的影响
 */
public class AcksConfigurationAnalysis {
    
    /**
     * 危险配置：acks=0（不等待任何确认）
     */
    public KafkaProducer<String, String> createDangerousProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 危险配置：不等待任何确认，性能最高但可靠性最低
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        
        return new KafkaProducer<>(props);
    }
    
    /**
     * 中等安全配置：acks=1（等待Leader确认）
     */
    public KafkaProducer<String, String> createModerateSafeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 中等安全：等待Leader副本确认
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        
        return new KafkaProducer<>(props);
    }
    
    /**
     * 最安全配置：acks=all（等待所有ISR副本确认）
     */
    public KafkaProducer<String, String> createSafestProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 最安全配置：等待所有ISR副本确认
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new KafkaProducer<>(props);
    }
}
```

</details>

#### 3. **重试配置不当**

**危险配置**：
   - `retries=0`：不进行重试，网络抖动时容易丢失消息
   - `retry.backoff.ms`过小：重试间隔太短，可能导致重试失败
   - `delivery.timeout.ms`过小：总超时时间不足，重试次数受限

**安全配置**：
   - `retries=Integer.MAX_VALUE`：允许足够的重试次数
   - `retry.backoff.ms=100`：合理的重试间隔
   - `delivery.timeout.ms=120000`：足够的总超时时间

### **Broker端消息丢失场景**

#### 1. **副本数量不足**

<details>
<summary>点击展开副本配置分析</summary>

```bash
# 危险配置：副本数为1（无备份）
kafka-topics.sh --create --topic dangerous-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1  # 危险：无副本备份

# 安全配置：副本数为3（推荐）
kafka-topics.sh --create --topic safe-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 3  # 安全：有副本备份
```

**Broker配置**：
```properties
# 危险配置
default.replication.factor=1  # 危险：默认无副本
min.insync.replicas=1        # 危险：只需要1个副本确认

# 安全配置
default.replication.factor=3  # 安全：默认3个副本
min.insync.replicas=2        # 安全：至少需要2个副本确认
```

</details>

#### 2. **刷盘策略不当**

<details>
<summary>点击展开刷盘策略配置</summary>

```properties
# Broker刷盘配置

# 危险配置：依赖操作系统刷盘
log.flush.interval.messages=9223372036854775807  # 危险：几乎不主动刷盘
log.flush.interval.ms=9223372036854775807       # 危险：几乎不主动刷盘

# 安全配置：定期强制刷盘
log.flush.interval.messages=10000  # 每10000条消息刷盘一次
log.flush.interval.ms=1000         # 每1秒刷盘一次

# 更安全的配置：每条消息都刷盘（性能较低）
log.flush.interval.messages=1      # 每条消息都刷盘
log.flush.interval.ms=1            # 每1毫秒刷盘一次
```

</details>

### **消费者端消息丢失场景**

#### 1. **自动提交偏移量时机问题**

<details>
<summary>点击展开自动提交问题分析</summary>

```java
/**
 * 自动提交导致消息丢失的场景分析
 */
public class AutoCommitMessageLoss {
    
    /**
     * 场景1：处理时间超过自动提交间隔
     */
    public void scenarioProcessingTimeExceedsCommitInterval() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "auto-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 危险配置：自动提交间隔短于消息处理时间
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000); // 5秒自动提交
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // 危险：消息处理时间10秒，超过自动提交间隔5秒
                    processLongRunningMessage(record); // 耗时10秒
                    
                } catch (Exception e) {
                    // 如果处理失败，但偏移量可能已经自动提交
                    System.err.println("消息处理失败，但可能已提交: " + record.offset());
                }
            }
        }
    }
    
    /**
     * 场景2：消费者异常退出
     */
    public void scenarioConsumerCrash() {
        Properties props = new Properties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    // 处理过程中可能发生系统崩溃
                    processMessage(record);
                    
                    // 如果在这里系统崩溃，已拉取但未处理完的消息会丢失
                    // 因为下次启动时会从已提交的偏移量开始消费
                }
            }
        } catch (Exception e) {
            // 异常退出，未处理完的消息可能丢失
            System.err.println("消费者异常退出: " + e.getMessage());
        }
    }
    
    private void processLongRunningMessage(ConsumerRecord<String, String> record) throws Exception {
        // 模拟长时间处理
        Thread.sleep(10000);
        System.out.println("处理完成: " + record.value());
    }
    
    private void processMessage(ConsumerRecord<String, String> record) {
        // 模拟可能崩溃的处理
        if (Math.random() < 0.01) {
            throw new RuntimeException("系统崩溃");
        }
        System.out.println("处理消息: " + record.value());
    }
}
```

</details>

#### 2. **消费者组重平衡时的丢失**

<details>
<summary>点击展开重平衡问题分析</summary>

```java
/**
 * 消费者组重平衡导致的消息丢失
 */
public class RebalanceMessageLoss {
    
    /**
     * 重平衡监听器，处理重平衡时的偏移量提交
     */
    public class SafeRebalanceListener implements ConsumerRebalanceListener {
        private final KafkaConsumer<String, String> consumer;
        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        
        public SafeRebalanceListener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }
        
        /**
         * 分区被撤销前的处理
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("分区被撤销，提交当前偏移量: " + partitions);
            
            // 在分区被撤销前，提交当前处理进度
            try {
                consumer.commitSync(currentOffsets);
                System.out.println("重平衡前偏移量提交成功");
            } catch (Exception e) {
                System.err.println("重平衡前偏移量提交失败: " + e.getMessage());
            }
        }
        
        /**
         * 分区被分配后的处理
         */
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            System.out.println("分区被分配: " + partitions);
            // 可以在这里重置偏移量或进行其他初始化操作
        }
        
        /**
         * 更新当前偏移量
         */
        public void updateOffset(TopicPartition partition, long offset) {
            currentOffsets.put(partition, new OffsetAndMetadata(offset + 1));
        }
    }
    
    /**
     * 安全的消费者实现，处理重平衡
     */
    public void safeConsumerWithRebalanceHandling() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "safe-rebalance-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 关闭自动提交，手动控制偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        SafeRebalanceListener rebalanceListener = new SafeRebalanceListener(consumer);
        
        // 订阅时指定重平衡监听器
        consumer.subscribe(Arrays.asList("test-topic"), rebalanceListener);
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // 处理消息
                        processMessage(record);
                        
                        // 更新偏移量到监听器
                        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                        rebalanceListener.updateOffset(partition, record.offset());
                        
                    } catch (Exception e) {
                        System.err.println("消息处理失败: " + record.offset());
                    }
                }
                
                // 定期提交偏移量
                if (!records.isEmpty()) {
                    try {
                        consumer.commitSync();
                    } catch (CommitFailedException e) {
                        System.err.println("偏移量提交失败: " + e.getMessage());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
    
    private void processMessage(ConsumerRecord<String, String> record) {
        System.out.println("处理消息: " + record.value());
    }
}
```

</details>

### **网络和系统层面的丢失**

#### 1. **网络分区**

**场景**：
   - 生产者与Broker网络中断，消息发送失败
   - Broker之间网络分区，副本同步失败
   - 消费者与Broker网络中断，无法消费消息

**预防措施**：
   - 配置合理的网络超时时间
   - 启用重试机制
   - 监控网络连接状态

#### 2. **磁盘故障**

**场景**：
   - Broker磁盘损坏，日志文件丢失
   - 磁盘空间不足，无法写入新消息

**预防措施**：
   - 配置多个日志目录（RAID）
   - 监控磁盘使用率
   - 定期备份重要数据

#### 3. **消息过期删除**

<details>
<summary>点击展开消息保留策略配置</summary>

```properties
# Broker消息保留配置

# 危险配置：保留时间过短
log.retention.hours=1        # 危险：只保留1小时
log.retention.bytes=1048576  # 危险：只保留1MB

# 安全配置：合理的保留时间
log.retention.hours=168      # 保留7天
log.retention.bytes=1073741824  # 保留1GB

# 针对重要主题的特殊配置
# 使用kafka-configs.sh命令为特殊主题设置更长的保留时间
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name important-topic \
  --alter \
  --add-config retention.ms=604800000  # 保留7天
```

</details>

### **开发中常见错误**

1. **生产者端错误**：
   - 使用fire-and-forget模式发送重要消息
   - acks配置为0或1，没有等待足够的副本确认
   - 重试次数设置过少
   - 程序退出前没有等待消息发送完成

2. **消费者端错误**：
   - 依赖自动提交处理重要消息
   - 没有处理重平衡事件
   - 异常处理不当，导致消息跳过
   - 没有实现幂等性处理

3. **配置错误**：
   - 副本数设置为1
   - min.insync.replicas设置过低
   - 消息保留时间过短
   - 没有监控磁盘空间

### **最佳实践**

1. **端到端可靠性配置**：
   - 生产者：acks=all，启用幂等性，足够的重试
   - Broker：副本数≥3，min.insync.replicas≥2
   - 消费者：手动提交偏移量，实现幂等性处理

2. **监控和告警**：
   - 监控消息发送成功率
   - 监控消费延迟
   - 监控磁盘使用率
   - 监控网络连接状态

3. **容错设计**：
   - 实现消息重试机制
   - 设计死信队列处理失败消息
   - 定期备份重要数据
   - 制定故障恢复预案

---

## **Kafka其他重要场景和注意事项**

### 问题：除了消息丢失和重复消费场景外，Kafka还有哪些需要注意的场景？

### **性能相关场景**

#### 1. **消息积压**

<details>
<summary>点击展开消息积压处理方案</summary>

```java
/**
 * 消息积压监控和处理
 */
public class MessageBacklogHandler {
    
    /**
     * 监控消费延迟
     */
    public void monitorConsumerLag() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lag-monitor-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        
        // 获取消费者组的分区分配信息
        consumer.subscribe(Arrays.asList("test-topic"));
        consumer.poll(Duration.ofMillis(0)); // 触发分区分配
        
        Set<TopicPartition> assignedPartitions = consumer.assignment();
        
        // 获取每个分区的最新偏移量
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignedPartitions);
        
        // 获取当前消费位置
        for (TopicPartition partition : assignedPartitions) {
            long currentOffset = consumer.position(partition);
            long endOffset = endOffsets.get(partition);
            long lag = endOffset - currentOffset;
            
            System.out.println(String.format(
                "分区 %s: 当前偏移量=%d, 最新偏移量=%d, 延迟=%d",
                partition, currentOffset, endOffset, lag
            ));
            
            // 设置告警阈值
            if (lag > 10000) {
                System.err.println("警告：分区 " + partition + " 消息积压严重，延迟 " + lag + " 条消息");
                // 触发告警或自动扩容
                handleHighLag(partition, lag);
            }
        }
        
        consumer.close();
    }
    
    /**
     * 处理高延迟情况
     */
    private void handleHighLag(TopicPartition partition, long lag) {
        // 方案1: 增加消费者实例
        // 方案2: 优化消息处理逻辑
        // 方案3: 临时增加分区数（需要重新分配）
        System.out.println("执行积压处理策略...");
    }
    
    /**
     * 批量消费提高吞吐量
     */
    public void batchConsumption() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 优化批量消费配置
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);     // 每次拉取更多消息
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 1024); // 最小拉取1MB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);     // 最大等待500ms
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            
            if (!records.isEmpty()) {
                System.out.println("批量处理 " + records.count() + " 条消息");
                
                // 批量处理消息
                processBatch(records);
                
                // 批量提交偏移量
                consumer.commitSync();
            }
        }
    }
    
    /**
     * 批量处理消息
     */
    private void processBatch(ConsumerRecords<String, String> records) {
        List<String> messages = new ArrayList<>();
        
        for (ConsumerRecord<String, String> record : records) {
            messages.add(record.value());
        }
        
        // 批量处理业务逻辑
        System.out.println("批量处理 " + messages.size() + " 条消息");
    }
}
```

</details>

#### 2. **分区热点**

**常见原因**：
   - 分区键设计不合理，导致数据倾斜
   - 某些分区的消费者处理能力不足
   - 分区数量设置不当

**解决方案**：
   - 重新设计分区键，确保数据均匀分布
   - 增加分区数量（需要重新分配）
   - 优化消费者处理逻辑
   - 使用自定义分区器

#### 3. **内存溢出**

<details>
<summary>点击展开内存优化配置</summary>

```java
/**
 * 内存优化配置
 */
public class MemoryOptimization {
    
    /**
     * 生产者内存优化配置
     */
    public KafkaProducer<String, String> createMemoryOptimizedProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 内存优化配置
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);    // 32MB缓冲区
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);                  // 16KB批次大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);                       // 5ms等待时间
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");         // 启用压缩
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);               // 最大阻塞时间
        
        return new KafkaProducer<>(props);
    }
    
    /**
     * 消费者内存优化配置
     */
    public KafkaConsumer<String, String> createMemoryOptimizedConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "memory-optimized-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 内存优化配置
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);              // 最小拉取1KB
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 50 * 1024 * 1024);  // 最大拉取50MB
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);              // 每次最多500条
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);         // 64KB接收缓冲区
        props.put(ConsumerConfig.SEND_BUFFER_CONFIG, 128 * 1024);           // 128KB发送缓冲区
        
        return new KafkaConsumer<>(props);
    }
}
```

</details>

### **数据一致性场景**

#### 1. **消费者组重平衡**

**触发条件**：
   - 消费者加入或离开消费者组
   - 分区数量发生变化
   - 消费者心跳超时

**影响**：
   - 短暂的消费中断
   - 可能的消息重复消费
   - 分区重新分配

**优化策略**：
   - 合理设置心跳间隔
   - 实现重平衡监听器
   - 避免频繁的消费者变动

#### 2. **事务处理**

<details>
<summary>点击展开事务处理示例</summary>

```java
/**
 * Kafka事务处理
 */
public class KafkaTransactionExample {
    
    /**
     * 事务性生产者
     */
    public void transactionalProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 事务配置
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-producer-1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // 初始化事务
        producer.initTransactions();
        
        try {
            // 开始事务
            producer.beginTransaction();
            
            // 发送消息
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("topic1", "key" + i, "value" + i));
                producer.send(new ProducerRecord<>("topic2", "key" + i, "value" + i));
            }
            
            // 提交事务
            producer.commitTransaction();
            System.out.println("事务提交成功");
            
        } catch (Exception e) {
            // 回滚事务
            producer.abortTransaction();
            System.err.println("事务回滚: " + e.getMessage());
        } finally {
            producer.close();
        }
    }
    
    /**
     * 事务性消费者
     */
    public void transactionalConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "transaction-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // 事务隔离级别
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("topic1", "topic2"));
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            
            for (ConsumerRecord<String, String> record : records) {
                // 只会消费已提交的事务消息
                System.out.println("消费事务消息: " + record.value());
            }
            
            // 手动提交偏移量
            consumer.commitSync();
        }
    }
}
```

</details>

### **运维和监控场景**

#### 1. **磁盘空间不足**

**监控指标**：
   - 磁盘使用率
   - 日志文件大小
   - 消息保留时间

**预防措施**：
   - 设置合理的日志保留策略
   - 监控磁盘使用率告警
   - 定期清理过期日志

#### 2. **网络分区和脑裂**

**场景描述**：
   - Broker之间网络中断
   - Zookeeper集群分区
   - 客户端无法连接到Broker

**预防措施**：
   - 部署多个可用区
   - 配置合理的网络超时
   - 监控网络连接状态

#### 3. **Zookeeper连接问题**

**常见问题**：
   - Zookeeper会话超时
   - Zookeeper集群不可用
   - 网络延迟导致连接不稳定

**解决方案**：
   - 增加Zookeeper会话超时时间
   - 部署高可用Zookeeper集群
   - 监控Zookeeper连接状态

### **安全相关场景**

#### 1. **认证授权**

<details>
<summary>点击展开安全配置示例</summary>

```properties
# Broker安全配置
# SASL认证配置
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN

# SSL配置
ssl.keystore.location=/path/to/kafka.server.keystore.jks
ssl.keystore.password=password
ssl.key.password=password
ssl.truststore.location=/path/to/kafka.server.truststore.jks
ssl.truststore.password=password

# ACL配置
authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer
super.users=User:admin
```

```java
/**
 * 安全客户端配置
 */
public class SecureKafkaClient {
    
    /**
     * 创建安全的生产者
     */
    public KafkaProducer<String, String> createSecureProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 安全配置
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"user\" password=\"password\";");
        
        // SSL配置
        props.put("ssl.truststore.location", "/path/to/client.truststore.jks");
        props.put("ssl.truststore.password", "password");
        
        return new KafkaProducer<>(props);
    }
}
```

</details>

#### 2. **数据加密**

**传输加密**：
   - 使用SSL/TLS加密网络传输
   - 配置客户端和Broker的SSL证书

**存储加密**：
   - 使用文件系统级别的加密
   - 应用层消息加密

### **版本兼容性场景**

#### 1. **客户端版本兼容**

**注意事项**：
   - 新版本客户端向后兼容
   - 旧版本客户端可能不支持新特性
   - API变更可能导致兼容性问题

**最佳实践**：
   - 保持客户端和Broker版本同步
   - 升级前测试兼容性
   - 逐步升级，避免大版本跳跃

#### 2. **滚动升级**

**升级步骤**：
1. 升级Zookeeper集群
2. 逐个升级Broker节点
3. 升级客户端应用
4. 验证功能正常

### **消息顺序性场景**

#### 1. **全局顺序 vs 分区顺序**

**全局顺序**：
   - 只使用一个分区
   - 性能受限
   - 适合对顺序要求严格的场景

**分区顺序**：
   - 同一分区内保证顺序
   - 性能较好
   - 需要合理设计分区键

#### 2. **大消息处理**

**问题**：
   - 消息大小超过Broker限制
   - 网络传输效率低
   - 内存占用过大

**解决方案**：
   - 消息分片处理
   - 使用外部存储（如HDFS）存储大文件
   - 增加消息大小限制配置

### **开发中常见错误**

1. **性能相关错误**：
   - 分区数设置不合理
   - 批次大小配置不当
   - 没有启用压缩
   - 忽略消费延迟监控

2. **可靠性相关错误**：
   - 没有处理重平衡事件
   - 忽略网络异常处理
   - 没有实现故障恢复机制
   - 缺少监控和告警

3. **安全相关错误**：
   - 使用明文传输敏感数据
   - 没有配置访问控制
   - 忽略安全更新
   - 密码硬编码在代码中

### **最佳实践**

1. **性能优化**：
   - 合理设置分区数和副本数
   - 启用压缩和批处理
   - 监控关键性能指标
   - 定期进行性能测试

2. **可靠性保证**：
   - 实现完整的错误处理机制
   - 配置合理的超时和重试
   - 建立监控和告警体系
   - 制定故障恢复预案

3. **安全防护**：
   - 启用认证和授权
   - 使用SSL/TLS加密传输
   - 定期更新安全补丁
   - 实施最小权限原则

4. **运维管理**：
   - 建立完善的监控体系
   - 制定标准的运维流程
   - 定期备份重要配置
   - 建立容量规划机制