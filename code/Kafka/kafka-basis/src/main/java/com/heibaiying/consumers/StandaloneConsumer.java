package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 独立消费者示例
 * 
 * 【独立消费者核心概念】
 * 独立消费者(Standalone Consumer)是指不加入消费者组，而是直接指定要消费的分区的消费者。
 * 与消费者组模式不同，独立消费者需要手动管理分区分配和偏移量提交。
 * 
 * 【独立消费者 vs 消费者组】
 * 1. 分区分配：
 *    - 消费者组：由Kafka自动分配分区，支持动态再均衡
 *    - 独立消费者：手动指定要消费的分区，无自动再均衡
 * 
 * 2. 偏移量管理：
 *    - 消费者组：可以自动或手动提交偏移量到__consumer_offsets主题
 *    - 独立消费者：必须手动管理偏移量，可以提交到Kafka或外部存储
 * 
 * 3. 扩展性：
 *    - 消费者组：支持动态添加/移除消费者，自动重新分配分区
 *    - 独立消费者：需要手动管理消费者实例和分区分配
 * 
 * 【适用场景】
 * 1. 需要精确控制分区消费的场景
 * 2. 简单的单实例消费场景
 * 3. 需要自定义偏移量存储的场景
 * 4. 对消费顺序有严格要求的场景
 * 
 * 【注意事项】
 * 1. 独立消费者不会触发再均衡，适合固定的消费模式
 * 2. 需要手动处理消费者故障和恢复
 * 3. 分区数量变化时需要手动调整消费逻辑
 * 4. 偏移量管理完全由应用程序负责
 */
public class StandaloneConsumer {

    public static void main(String[] args) {
        // 指定要消费的主题名称
        String topic = "Kafka-Partitioner-Test";
        // 消费者组ID（独立消费者中主要用于偏移量存储标识）
        String group = "group1";
        
        // 配置消费者属性
        Properties props = new Properties();
        // Kafka集群地址
        props.put("bootstrap.servers", "hadoop001:9092");
        // 消费者组ID，即使是独立消费者也需要设置（用于偏移量管理）
        props.put("group.id", group);
        // 禁用自动提交偏移量，改为手动控制
        props.put("enable.auto.commit", false);
        // 键的反序列化器：将字节数组转换为Integer类型
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        // 值的反序列化器：将字节数组转换为String类型
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(props);

        // 创建要消费的分区列表
        List<TopicPartition> partitions = new ArrayList<>();
        
        // 获取指定主题的所有分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        System.out.println("主题 " + topic + " 的分区信息：");
        for (PartitionInfo info : partitionInfos) {
            System.out.println("分区 " + info.partition() + ", Leader: " + info.leader());
        }

        // 【独立消费者核心】：手动指定要消费的分区
        // 这里演示只消费分区0，实际应用中可以根据需要选择特定分区
        for (PartitionInfo partition : partitionInfos) {
            if (partition.partition() == 0) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
                System.out.println("选择消费分区: " + partition.partition());
            }
        }

        // 【关键步骤】为消费者分配指定的分区
        // 注意：使用assign()而不是subscribe()，这是独立消费者的标志
        consumer.assign(partitions);
        
        System.out.println("独立消费者启动，开始消费指定分区的消息...");


        try {
            // 消费消息的主循环
            while (true) {
                // 拉取消息，设置超时时间为100毫秒
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理拉取到的消息
                for (ConsumerRecord<Integer, String> record : records) {
                    // 打印消息详细信息
                    System.out.printf("[独立消费者] 分区=%d, 偏移量=%d, 键=%d, 值=%s, 时间戳=%d\n",
                            record.partition(), record.offset(), record.key(), 
                            record.value(), record.timestamp());
                }
                
                // 【重要】手动同步提交偏移量
                // 独立消费者必须手动管理偏移量，确保消息不会重复消费
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    System.out.println("偏移量已提交，处理了 " + records.count() + " 条消息");
                }
            }
        } catch (Exception e) {
            System.err.println("消费过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 确保消费者正确关闭
            System.out.println("关闭独立消费者...");
            consumer.close();
        }

    }
}
