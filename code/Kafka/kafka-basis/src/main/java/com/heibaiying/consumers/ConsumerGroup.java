package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka消费者组（Consumer Group）基础示例
 * 
 * 知识点说明：
 * 1. 消费者组的核心概念：
 *    - 消费者组是Kafka中消费者的逻辑分组
 *    - 同一个消费者组内的消费者共同消费一个或多个主题
 *    - 每个分区只能被同一消费者组内的一个消费者消费
 *    - 不同消费者组可以独立消费同一个主题的所有消息
 * 
 * 2. 分区分配策略：
 *    - Range策略：按分区范围分配（默认）
 *    - RoundRobin策略：轮询分配
 *    - Sticky策略：尽量保持原有分配，减少重平衡开销
 *    - 当消费者数量变化时，会触发重平衡（Rebalance）
 * 
 * 3. 自动提交偏移量：
 *    - enable.auto.commit=true：启用自动提交
 *    - auto.commit.interval.ms：自动提交间隔（默认5秒）
 *    - 优点：简化编程模型，无需手动管理偏移量
 *    - 缺点：可能导致消息丢失或重复消费
 * 
 * 4. 消费者组的优势：
 *    - 水平扩展：增加消费者可以提高消费能力
 *    - 容错性：某个消费者失败时，其他消费者可以接管
 *    - 负载均衡：自动分配分区给不同消费者
 *    - 独立消费：不同消费者组互不影响
 * 
 * 5. 适用场景：
 *    - 需要水平扩展消费能力的应用
 *    - 多个应用需要独立消费同一主题
 *    - 需要容错和负载均衡的消费场景
 *    - 简单的消费场景（使用自动提交）
 * 
 * @author heibaiying
 */
public class ConsumerGroup {

    public static void main(String[] args) {
        // 指定要消费的主题名称
        String topic = "Hello-Kafka";
        
        // 指定消费者组ID
        // 重要：同一个group.id的消费者属于同一个消费者组
        // 不同的group.id可以独立消费同一个主题的所有消息
        String group = "group1";
        
        // 配置消费者属性
        Properties props = new Properties();
        
        // Kafka集群地址
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 消费者组ID - 这是消费者组的核心配置
        // 同一个消费者组内的消费者会协调消费分区
        props.put("group.id", group);
        
        // 启用自动提交偏移量
        // true：Kafka会定期自动提交偏移量，简化编程
        // false：需要手动提交偏移量，提供更精确的控制
        props.put("enable.auto.commit", true);
        
        // 自动提交偏移量的时间间隔（毫秒）
        // 默认值是5000ms（5秒），可以根据需要调整
        props.put("auto.commit.interval.ms", "1000");
        
        // 当没有初始偏移量或偏移量超出范围时的处理策略
        // earliest：从最早的消息开始消费
        // latest：从最新的消息开始消费（默认）
        // none：如果没有找到偏移量则抛出异常
        props.put("auto.offset.reset", "earliest");
        
        // 键和值的反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅指定主题
        // 注意：一个消费者可以订阅多个主题
        // 例如：consumer.subscribe(Arrays.asList("topic1", "topic2"));
        consumer.subscribe(Collections.singletonList(topic));
        
        System.out.println("消费者组 '" + group + "' 开始消费主题 '" + topic + "'");
        System.out.println("消费者ID: " + consumer.groupMetadata().memberId());

        try {
            // 消费消息的主循环
            while (true) {
                // 轮询获取消息数据
                // poll()方法是消费者的核心方法，用于从Kafka拉取消息
                // 参数指定最长等待时间，如果没有消息会阻塞直到超时
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理获取到的消息
                if (!records.isEmpty()) {
                    System.out.println("本次拉取到 " + records.count() + " 条消息");
                }
                
                // 遍历处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息的详细信息
                    System.out.printf("消费消息 - topic=%s, partition=%d, key=%s, value=%s, offset=%d, timestamp=%d%n",
                            record.topic(), 
                            record.partition(), 
                            record.key(), 
                            record.value(), 
                            record.offset(),
                            record.timestamp());
                    
                    // 这里可以添加具体的业务处理逻辑
                    // 例如：数据处理、存储到数据库、调用其他服务等
                    
                    // 模拟业务处理时间
                    try {
                        Thread.sleep(10); // 模拟处理耗时
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                
                // 由于启用了自动提交，偏移量会自动提交
                // 无需手动调用 consumer.commitSync() 或 consumer.commitAsync()
            }
        } catch (Exception e) {
            // 异常处理
            System.err.println("消费过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                // 关闭消费者前的清理工作
                System.out.println("正在关闭消费者...");
                
                // 关闭消费者，这会触发以下操作：
                // 1. 离开消费者组，触发重平衡
                // 2. 提交未提交的偏移量（如果启用了自动提交）
                // 3. 关闭网络连接，释放资源
                consumer.close();
                
                System.out.println("消费者已关闭");
            } catch (Exception e) {
                System.err.println("关闭消费者时发生异常: " + e.getMessage());
            }
        }
    }
}
