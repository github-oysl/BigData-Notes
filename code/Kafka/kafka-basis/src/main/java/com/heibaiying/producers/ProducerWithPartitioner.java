package com.heibaiying.producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Kafka生产者使用自定义分区器示例
 * 
 * 知识点说明：
 * 1. 自定义分区器：通过实现Partitioner接口来自定义消息分区逻辑
 * 2. 分区器配置：通过partitioner.class属性指定自定义分区器类
 * 3. 参数传递：可以通过Properties向分区器传递自定义参数
 * 4. 分区效果：不同的分区策略会影响消息在各分区间的分布
 * 
 * 注意事项：
 * - 自定义分区器类必须在classpath中可访问
 * - 分区器的参数通过Properties传递，在configure方法中获取
 * - 分区号从0开始，不能超过Topic的分区总数
 * - 合理的分区策略有助于提高消费者的并行处理能力
 * 
 * @author heibaiying
 */
public class ProducerWithPartitioner {

    /**
     * 主方法：演示使用自定义分区器发送消息
     * @param args 命令行参数
     */
    public static void main(String[] args) {

        // 定义测试Topic，建议创建时设置多个分区以观察分区效果
        String topicName = "Kafka-Partitioner-Test";

        // 创建生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 配置序列化器
        // 注意：这里Key使用IntegerSerializer，因为自定义分区器需要Integer类型的Key
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 配置自定义分区器
        // partitioner.class：指定自定义分区器的完整类名
        props.put("partitioner.class", "com.heibaiying.producers.partitioners.CustomPartitioner");
        
        // 传递分区器所需的自定义参数
        // 这个参数会在分区器的configure方法中通过configs.get("pass.line")获取
        props.put("pass.line", 6);

        // 创建生产者实例
        // 注意泛型类型：Key是Integer，Value是String
        Producer<Integer, String> producer = new KafkaProducer<>(props);

        System.out.println("开始发送消息，观察自定义分区器的分区效果...");
        
        // 发送分数从0到10的消息，测试分区器的分区逻辑
        for (int i = 0; i <= 10; i++) {
            String score = "score:" + i;
            
            // 创建消息记录，Key为分数值，Value为分数描述
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, i, score);
            
            // 异步发送消息，使用Lambda表达式处理回调
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("发送失败: " + exception.getMessage());
                } else {
                    // 打印消息和其被分配到的分区
                    // 可以观察到：分数>=6的消息分配到分区1，分数<6的消息分配到分区0
                    System.out.printf("消息: %s -> 分区: %d, 偏移量: %d\n", 
                            score, metadata.partition(), metadata.offset());
                }
            });
        }

        // 关闭生产者，等待所有消息发送完成
        producer.close();
        
        System.out.println("所有消息发送完成，生产者已关闭");
        System.out.println("分区规则：分数>=6的消息在分区1，分数<6的消息在分区0");
    }
}