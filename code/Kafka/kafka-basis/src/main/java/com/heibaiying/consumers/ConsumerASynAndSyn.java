package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka消费者——同步加异步提交（混合提交策略）
 * 
 * 知识点说明：
 * 1. 混合提交策略的优势：
 *    - 结合了异步提交的高性能和同步提交的可靠性
 *    - 正常运行时使用异步提交，提高吞吐量
 *    - 关闭消费者时使用同步提交，确保偏移量提交成功
 * 
 * 2. 提交策略对比：
 *    - 纯异步提交：性能最高，但可能丢失偏移量
 *    - 纯同步提交：可靠性最高，但性能较低
 *    - 混合提交：平衡性能和可靠性的最佳实践
 * 
 * 3. 适用场景：
 *    - 对性能有要求，同时不能容忍数据丢失的场景
 *    - 长时间运行的消费者应用
 *    - 生产环境中的推荐做法
 * 
 * 4. 注意事项：
 *    - finally块中的同步提交确保优雅关闭
 *    - 异常处理要考虑偏移量提交失败的情况
 *    - 可以结合回调函数进一步优化异步提交
 * 
 * @author heibaiying
 */
public class ConsumerASynAndSyn {

    public static void main(String[] args) {
        // 指定要消费的主题名称
        String topic = "Hello-Kafka";
        
        // 指定消费者组ID
        // 同一消费者组内的消费者会进行负载均衡，不会重复消费同一消息
        String group = "group1";
        
        // 配置消费者属性
        Properties props = new Properties();
        
        // Kafka集群地址
        // 注意：根据实际环境修改为正确的Kafka服务器地址
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 消费者组ID，用于标识消费者所属的组
        props.put("group.id", group);
        
        // 禁用自动提交偏移量
        // 设置为false后，需要手动控制偏移量提交时机
        props.put("enable.auto.commit", false);
        
        // 键的反序列化器：将字节数组转换为键对象
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 值的反序列化器：将字节数组转换为值对象
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅指定主题
        // Collections.singletonList()创建只包含一个元素的不可变列表
        consumer.subscribe(Collections.singletonList(topic));

        try {
            // 消费消息的主循环
            while (true) {
                // 拉取消息，设置超时时间为100毫秒
                // 如果没有消息可用，会等待指定时间后返回空记录集
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理拉取到的每条消息
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息详细信息：主题、分区、偏移量、键、值等
                    System.out.printf("接收到消息 - topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                
                // 异步提交偏移量
                // 优点：不会阻塞消费线程，提高吞吐量
                // 缺点：可能提交失败，导致重复消费
                // 适用于：正常运行时的偏移量提交
                consumer.commitAsync();
            }
        } catch (Exception e) {
            // 异常处理：记录错误信息
            System.err.println("消费消息时发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                // 同步提交偏移量
                // 在关闭消费者前进行最后一次偏移量提交
                // 使用同步提交确保偏移量提交成功，避免重复消费
                System.out.println("正在进行最终的同步偏移量提交...");
                consumer.commitSync();
                System.out.println("同步偏移量提交成功");
            } catch (Exception e) {
                // 同步提交失败的处理
                System.err.println("最终同步提交偏移量失败: " + e.getMessage());
                e.printStackTrace();
            } finally {
                // 关闭消费者，释放资源
                // 这会触发消费者离开消费者组，触发重新平衡
                System.out.println("正在关闭消费者...");
                consumer.close();
                System.out.println("消费者已关闭");
            }
        }
    }
}
