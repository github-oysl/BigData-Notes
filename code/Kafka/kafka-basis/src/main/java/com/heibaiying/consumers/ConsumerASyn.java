package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka消费者异步提交示例
 * 
 * 知识点说明：
 * 1. 异步提交：commitAsync()不会阻塞，提高消费性能
 * 2. 偏移量管理：手动控制偏移量提交，避免消息丢失或重复消费
 * 3. 回调处理：通过OffsetCommitCallback处理提交结果
 * 4. 消费者组：同一组内的消费者会自动进行负载均衡
 * 
 * 注意事项：
 * - 异步提交可能失败，需要在回调中处理异常情况
 * - 关闭消费者前建议进行同步提交确保偏移量已保存
 * - enable.auto.commit=false时必须手动提交偏移量
 * - 异步提交的顺序可能与调用顺序不同
 * 
 * @author heibaiying
 */
public class ConsumerASyn {

    /**
     * 主方法：演示异步提交偏移量的消费者
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        // 定义要消费的Topic
        String topic = "Hello-Kafka";
        
        // 定义消费者组ID
        // 同一组内的消费者会共同消费Topic的所有分区，实现负载均衡
        String group = "group1";
        
        // 创建消费者配置
        Properties props = new Properties();
        
        // 配置Kafka集群地址
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 配置消费者组ID（必需配置）
        // 消费者组是Kafka实现负载均衡和故障转移的基础
        props.put("group.id", group);
        
        // 禁用自动提交偏移量（重要配置）
        // 设置为false后，需要手动调用commit方法提交偏移量
        // 这样可以更精确地控制何时提交，避免消息丢失
        props.put("enable.auto.commit", false);
        
        // 配置Key反序列化器
        // 将从Kafka接收到的字节数组转换为Java对象
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 配置Value反序列化器
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅Topic
        // Collections.singletonList()创建只包含一个元素的列表
        // 也可以订阅多个Topic：Arrays.asList("topic1", "topic2")
        consumer.subscribe(Collections.singletonList(topic));
        
        System.out.println("开始消费消息，使用异步提交偏移量...");

        try {
            // 消费循环
            while (true) {
                // 拉取消息
                // poll()方法从Kafka拉取消息，参数是超时时间
                // Duration.of(100, ChronoUnit.MILLIS)表示100毫秒超时
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理拉取到的消息
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息详细信息
                    System.out.printf("接收消息: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    
                    // 这里可以添加业务逻辑处理消息
                    // 例如：保存到数据库、发送到其他系统等
                }
                
                // 异步提交偏移量并定义回调函数
                // commitAsync()是非阻塞的，不会等待提交完成就返回
                // 这样可以提高消费性能，但需要通过回调处理提交结果
                consumer.commitAsync(new OffsetCommitCallback() {
                    /**
                     * 偏移量提交完成后的回调方法
                     * @param offsets 提交的偏移量信息，包含每个分区的偏移量
                     * @param exception 提交过程中的异常，成功时为null
                     */
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            // 提交失败，进行错误处理
                            System.err.println("偏移量提交失败: " + exception.getMessage());
                            
                            // 打印失败的偏移量信息，便于排查问题
                            offsets.forEach((topicPartition, offsetMetadata) -> 
                                System.err.printf("提交失败 -> Topic=%s, Partition=%d, Offset=%s%n",
                                    topicPartition.topic(), topicPartition.partition(), offsetMetadata.offset()));
                        } else {
                            // 提交成功
                            System.out.println("偏移量异步提交成功，提交的分区数: " + offsets.size());
                        }
                    }
                });
            }
        } finally {
            // 关闭消费者，释放资源
            // 在关闭前，消费者会尝试提交当前的偏移量
            // 建议在finally块中进行同步提交以确保偏移量被保存
            try {
                consumer.commitSync(); // 同步提交，确保偏移量被保存
                System.out.println("消费者关闭前已同步提交偏移量");
            } catch (Exception e) {
                System.err.println("最终同步提交失败: " + e.getMessage());
            }
            consumer.close();
            System.out.println("消费者已关闭");
        }
    }
}
