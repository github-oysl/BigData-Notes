package com.heibaiying.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Flink Kafka 连接器示例
 * 
 * 功能说明：
 * 1. 从Kafka主题读取数据
 * 2. 对数据进行处理和转换
 * 3. 将处理结果写入另一个Kafka主题
 * 
 * 适用场景：
 * - 实时数据管道构建
 * - 消息队列数据处理
 * - 微服务间数据传输
 * - 实时ETL处理
 * - 事件驱动架构
 * 
 * 注意：此示例需要Kafka连接器依赖，需要在pom.xml中添加相应依赖
 * 
 * @author heibaiying
 */
public class KafkaConnectorExample {

    /**
     * 程序入口
     * 
     * @param args 命令行参数
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        
        // 1. 获取流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 启用检查点，确保数据的精确一次处理
        env.enableCheckpointing(5000);
        
        // 2. 配置Kafka消费者属性
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");  // Kafka服务器地址
        consumerProps.setProperty("group.id", "flink-consumer-group");     // 消费者组ID
        consumerProps.setProperty("auto.offset.reset", "latest");          // 从最新位置开始消费
        consumerProps.setProperty("enable.auto.commit", "false");          // 禁用自动提交，由Flink管理偏移量
        
        // 3. 创建Kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "input-topic",           // 输入主题名称
            new SimpleStringSchema(), // 序列化器
            consumerProps            // 消费者配置
        );
        
        // 设置从最新偏移量开始消费（可选）
        kafkaConsumer.setStartFromLatest();
        
        // 4. 从Kafka读取数据流
        DataStream<String> inputStream = env.addSource(kafkaConsumer);
        
        // 5. 数据处理和转换
        DataStream<String> processedStream = inputStream
            .map(new DataProcessor())  // 数据处理
            .filter(value -> value != null && !value.isEmpty());  // 过滤空值
        
        // 6. 配置Kafka生产者属性
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");  // Kafka服务器地址
        producerProps.setProperty("transaction.timeout.ms", "300000");     // 事务超时时间
        
        // 7. 创建Kafka生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
            "output-topic",          // 输出主题名称
            new SimpleStringSchema(), // 序列化器
            producerProps,           // 生产者配置
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE  // 精确一次语义
        );
        
        // 8. 将处理结果写入Kafka
        processedStream.addSink(kafkaProducer);
        
        // 9. 同时输出到控制台用于调试
        processedStream.print("处理结果");
        
        // 10. 启动执行
        env.execute("Kafka Connector Example");
    }
    
    /**
     * 数据处理器
     * 对从Kafka读取的数据进行处理和转换
     */
    public static class DataProcessor implements MapFunction<String, String> {
        
        @Override
        public String map(String value) throws Exception {
            try {
                // 1. 数据清洗：去除前后空格
                String cleaned = value.trim();
                
                // 2. 数据验证：检查是否为空
                if (cleaned.isEmpty()) {
                    return null;
                }
                
                // 3. 数据转换：转换为大写并添加时间戳
                String processed = String.format("[%d] %s", 
                    System.currentTimeMillis(), 
                    cleaned.toUpperCase()
                );
                
                // 4. 数据增强：添加处理标识
                return "PROCESSED: " + processed;
                
            } catch (Exception e) {
                // 异常处理：记录错误并返回错误信息
                System.err.println("处理数据时发生错误: " + e.getMessage());
                return "ERROR: " + value;
            }
        }
    }
}

/*
依赖配置：
需要在pom.xml中添加Kafka连接器依赖：

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka_2.11</artifactId>
    <version>1.9.0</version>
</dependency>

使用说明：

1. 启动Kafka服务：
   # 启动Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # 启动Kafka
   bin/kafka-server-start.sh config/server.properties

2. 创建Kafka主题：
   # 创建输入主题
   bin/kafka-topics.sh --create --topic input-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   
   # 创建输出主题
   bin/kafka-topics.sh --create --topic output-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

3. 启动Flink程序

4. 向输入主题发送消息：
   bin/kafka-console-producer.sh --topic input-topic --bootstrap-server localhost:9092
   
   然后输入测试数据：
   hello world
   flink kafka integration
   real time processing

5. 查看输出主题的消息：
   bin/kafka-console-consumer.sh --topic output-topic --bootstrap-server localhost:9092 --from-beginning
   
   预期输出：
   PROCESSED: [1640995200000] HELLO WORLD
   PROCESSED: [1640995201000] FLINK KAFKA INTEGRATION
   PROCESSED: [1640995202000] REAL TIME PROCESSING

学习要点：
- FlinkKafkaConsumer：Kafka数据源
- FlinkKafkaProducer：Kafka数据汇
- 消费者组和偏移量管理
- 精确一次处理语义
- 检查点机制
- 序列化和反序列化
- 错误处理和容错机制

Kafka连接器配置要点：
1. bootstrap.servers：Kafka集群地址
2. group.id：消费者组标识
3. auto.offset.reset：偏移量重置策略
4. enable.auto.commit：是否自动提交偏移量
5. transaction.timeout.ms：事务超时时间
6. Semantic.EXACTLY_ONCE：精确一次语义保证

生产环境注意事项：
1. 合理设置并行度和分区数
2. 配置适当的检查点间隔
3. 监控消费延迟和吞吐量
4. 处理序列化异常
5. 配置合适的重试策略
6. 考虑数据倾斜问题
*/