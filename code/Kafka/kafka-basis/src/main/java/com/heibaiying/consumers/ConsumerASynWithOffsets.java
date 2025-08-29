package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka消费者——异步提交特定偏移量（手动偏移量管理）
 * 
 * 知识点说明：
 * 1. 手动偏移量管理的优势：
 *    - 精确控制偏移量提交时机和位置
 *    - 可以实现更细粒度的偏移量控制
 *    - 支持批量处理后统一提交偏移量
 *    - 避免消息处理失败但偏移量已提交的问题
 * 
 * 2. TopicPartition和OffsetAndMetadata：
 *    - TopicPartition：表示主题的特定分区
 *    - OffsetAndMetadata：包含偏移量和元数据信息
 *    - 偏移量值应该是下一条要消费的消息位置（当前偏移量+1）
 * 
 * 3. 适用场景：
 *    - 需要精确控制偏移量提交的场景
 *    - 批量处理消息后统一提交
 *    - 消息处理可能失败需要重试的场景
 *    - 需要实现"至少一次"语义的应用
 * 
 * 4. 注意事项：
 *    - 偏移量计算要正确（当前偏移量+1）
 *    - TopicPartition作为Map的key，利用其hashCode和equals方法
 *    - 要处理偏移量提交失败的情况
 *    - 避免偏移量提交过于频繁影响性能
 * 
 * @author heibaiying
 */
public class ConsumerASynWithOffsets {

    public static void main(String[] args) {
        // 指定要消费的主题名称
        String topic = "Hello-Kafka";
        
        // 指定消费者组ID
        String group = "group1";
        
        // 配置消费者属性
        Properties props = new Properties();
        
        // Kafka集群地址
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 消费者组ID
        props.put("group.id", group);
        
        // 禁用自动提交偏移量，改为手动控制
        props.put("enable.auto.commit", false);
        
        // 键和值的反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅指定主题
        consumer.subscribe(Collections.singletonList(topic));

        // 用于存储每个分区的偏移量信息
        // Key: TopicPartition（主题分区）
        // Value: OffsetAndMetadata（偏移量和元数据）
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        try {
            // 消费消息的主循环
            while (true) {
                // 拉取消息，超时时间100毫秒
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理每条消息并记录偏移量
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息详细信息
                    System.out.printf("处理消息 - topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    
                    // 创建TopicPartition对象，表示消息所属的主题分区
                    // TopicPartition重写了hashCode和equals方法，确保同一主题分区的实例相等
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    
                    // 创建OffsetAndMetadata对象
                    // 注意：偏移量要设置为当前消息偏移量+1，表示下一条要消费的消息位置
                    // 第二个参数是元数据，可以存储一些额外信息，这里使用简单字符串
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, "processed at " + System.currentTimeMillis());
                    
                    // 将分区偏移量信息存储到Map中
                    // 如果同一分区有多条消息，后面的偏移量会覆盖前面的，这样确保提交的是最新的偏移量
                    offsets.put(topicPartition, offsetAndMetadata);
                }
                
                // 异步提交特定偏移量
                // 第一个参数：要提交的偏移量Map
                // 第二个参数：回调函数（这里设为null，表示不处理提交结果）
                if (!offsets.isEmpty()) {
                    consumer.commitAsync(offsets, (offsetsMap, exception) -> {
                        if (exception != null) {
                            // 偏移量提交失败的处理
                            System.err.println("偏移量提交失败: " + exception.getMessage());
                            // 可以在这里实现重试逻辑或记录失败信息
                        } else {
                            // 偏移量提交成功
                            System.out.println("成功提交偏移量: " + offsetsMap);
                        }
                    });
                    
                    // 清空偏移量Map，为下一批消息做准备
                    offsets.clear();
                }
            }
        } catch (Exception e) {
            // 异常处理
            System.err.println("消费消息时发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                // 在关闭消费者前，如果还有未提交的偏移量，进行最后一次同步提交
                if (!offsets.isEmpty()) {
                    System.out.println("正在进行最终的偏移量提交...");
                    consumer.commitSync(offsets);
                    System.out.println("最终偏移量提交成功");
                }
            } catch (Exception e) {
                System.err.println("最终偏移量提交失败: " + e.getMessage());
            } finally {
                // 关闭消费者，释放资源
                System.out.println("正在关闭消费者...");
                consumer.close();
                System.out.println("消费者已关闭");
            }
        }
    }
}
