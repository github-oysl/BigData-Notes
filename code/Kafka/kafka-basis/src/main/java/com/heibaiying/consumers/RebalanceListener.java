package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Kafka消费者——重平衡监听器（Rebalance Listener）示例
 * 
 * 知识点说明：
 * 1. 重平衡（Rebalance）机制：
 *    - 当消费者组成员发生变化时触发（加入、离开、崩溃）
 *    - 当主题分区数量发生变化时触发
 *    - 重新分配分区给消费者组内的消费者
 *    - 确保每个分区只被一个消费者消费
 * 
 * 2. 重平衡监听器的作用：
 *    - 在重平衡前后执行自定义逻辑
 *    - 确保偏移量正确提交，避免消息丢失或重复
 *    - 清理资源或初始化状态
 *    - 记录重平衡事件用于监控和调试
 * 
 * 3. 回调方法说明：
 *    - onPartitionsRevoked：分区被撤销前调用
 *    - onPartitionsAssigned：分区被分配后调用
 *    - onPartitionsLost：分区丢失时调用（较新版本）
 * 
 * 4. 最佳实践：
 *    - 在onPartitionsRevoked中同步提交偏移量
 *    - 避免在回调方法中执行耗时操作
 *    - 正确处理异常，避免影响重平衡过程
 *    - 记录重平衡事件用于监控
 * 
 * 5. 适用场景：
 *    - 需要精确控制偏移量提交的应用
 *    - 需要在重平衡时进行状态管理的应用
 *    - 需要监控重平衡事件的生产环境
 *    - 有状态的流处理应用
 * 
 * @author heibaiying
 */
public class RebalanceListener {

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
        // 这样可以在重平衡时精确控制偏移量提交时机
        props.put("enable.auto.commit", false);
        
        // 设置会话超时时间（毫秒）
        // 如果消费者在此时间内没有发送心跳，会被认为已死亡，触发重平衡
        props.put("session.timeout.ms", "30000");
        
        // 设置心跳间隔时间（毫秒）
        // 消费者向协调器发送心跳的频率
        props.put("heartbeat.interval.ms", "10000");
        
        // 设置最大轮询间隔时间（毫秒）
        // 如果消费者在此时间内没有调用poll()，会被认为已死亡
        props.put("max.poll.interval.ms", "300000");
        
        // 键和值的反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 用于存储每个分区的偏移量信息
        // 这个Map在重平衡时用于提交偏移量
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        // 订阅主题并设置重平衡监听器
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

            /**
             * 分区撤销回调方法
             * 该方法会在消费者停止读取消息之后，重平衡开始之前被调用
             * 这是提交偏移量的最后机会，确保不会丢失已处理的消息
             * 
             * @param partitions 即将被撤销的分区集合
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("=== 重平衡即将开始 ===");
                System.out.println("即将撤销的分区: " + partitions);
                
                try {
                    // 在分区被撤销前，同步提交已处理消息的偏移量
                    // 这是非常重要的步骤，确保不会重复消费已处理的消息
                    if (!offsets.isEmpty()) {
                        System.out.println("正在提交偏移量: " + offsets);
                        consumer.commitSync(offsets);
                        System.out.println("偏移量提交成功");
                        
                        // 清空偏移量Map，为新的分区分配做准备
                        offsets.clear();
                    } else {
                        System.out.println("没有需要提交的偏移量");
                    }
                } catch (Exception e) {
                    System.err.println("提交偏移量时发生异常: " + e.getMessage());
                    e.printStackTrace();
                }
                
                // 记录重平衡开始时间，用于监控重平衡耗时
                System.out.println("重平衡开始时间: " + new Date());
            }

            /**
             * 分区分配回调方法
             * 该方法会在重新分配分区之后，消费者开始读取消息之前被调用
             * 可以在这里进行一些初始化工作
             * 
             * @param partitions 新分配的分区集合
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("=== 重平衡完成 ===");
                System.out.println("新分配的分区: " + partitions);
                System.out.println("重平衡完成时间: " + new Date());
                
                // 可以在这里进行一些初始化工作
                // 例如：重置状态、初始化缓存、记录分区分配信息等
                
                // 打印每个分区的当前偏移量信息
                for (TopicPartition partition : partitions) {
                    try {
                        // 获取分区的当前偏移量位置
                        long position = consumer.position(partition);
                        System.out.println("分区 " + partition + " 的当前偏移量: " + position);
                    } catch (Exception e) {
                        System.err.println("获取分区偏移量失败: " + e.getMessage());
                    }
                }
                
                System.out.println("准备开始消费新分配的分区");
            }
        });
        
        System.out.println("消费者启动，等待分区分配...");

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
                    
                    // 这里可以添加具体的业务处理逻辑
                    // 例如：数据处理、存储到数据库、调用其他服务等
                    
                    // 创建TopicPartition对象，表示消息所属的主题分区
                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                    
                    // 创建OffsetAndMetadata对象，记录下一条要消费的消息位置
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1, 
                            "processed at " + System.currentTimeMillis());
                    
                    // 将分区偏移量信息存储到Map中
                    // TopicPartition重写了hashCode和equals方法，确保同一主题分区的实例相等
                    offsets.put(topicPartition, offsetAndMetadata);
                }
                
                // 异步提交偏移量
                // 在正常消费过程中使用异步提交以提高性能
                if (!offsets.isEmpty()) {
                    consumer.commitAsync(offsets, (offsetsMap, exception) -> {
                        if (exception != null) {
                            System.err.println("异步提交偏移量失败: " + exception.getMessage());
                        } else {
                            // 注意：这里不清空offsets，因为重平衡时还需要用到
                            // offsets会在onPartitionsRevoked中清空
                        }
                    });
                }
            }
        } catch (Exception e) {
            // 异常处理
            System.err.println("消费过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                // 在关闭消费者前，进行最后的偏移量提交
                if (!offsets.isEmpty()) {
                    System.out.println("正在进行最终的偏移量提交...");
                    consumer.commitSync(offsets);
                    System.out.println("最终偏移量提交成功");
                }
            } catch (Exception e) {
                System.err.println("最终偏移量提交失败: " + e.getMessage());
            } finally {
                // 关闭消费者，这会触发最后一次重平衡
                System.out.println("正在关闭消费者...");
                consumer.close();
                System.out.println("消费者已关闭");
            }
        }
    }
}
