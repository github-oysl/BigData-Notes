package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka消费者同步提交示例
 * 
 * 知识点说明：
 * 1. 同步提交：commitSync()会阻塞等待提交完成，确保偏移量已保存
 * 2. 可靠性：同步提交比异步提交更可靠，但性能较低
 * 3. 异常处理：同步提交失败会抛出异常，需要适当处理
 * 4. 偏移量管理：手动控制提交时机，避免消息丢失
 * 
 * 注意事项：
 * - 同步提交会阻塞线程，影响消费性能
 * - 提交失败时会抛出异常，需要捕获并处理
 * - 适合对数据一致性要求较高的场景
 * - 可以与异步提交结合使用，在关闭时进行同步提交
 * 
 * @author heibaiying
 */
public class ConsumerSyn {

    /**
     * 主方法：演示同步提交偏移量的消费者
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        // 定义要消费的Topic
        String topic = "Hello-Kafka";
        
        // 定义消费者组ID
        String group = "group1";
        
        // 创建消费者配置
        Properties props = new Properties();
        
        // 配置Kafka集群地址
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 配置消费者组ID（必需配置）
        props.put("group.id", group);
        
        // 禁用自动提交偏移量
        // 设置为false后，需要手动调用commit方法提交偏移量
        props.put("enable.auto.commit", false);
        
        // 配置Key反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 配置Value反序列化器
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // 创建Kafka消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅Topic
        consumer.subscribe(Collections.singletonList(topic));
        
        System.out.println("开始消费消息，使用同步提交偏移量...");

        try {
            // 消费循环
            while (true) {
                // 拉取消息
                // poll()方法从Kafka拉取消息，超时时间为100毫秒
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理拉取到的消息
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息详细信息
                    System.out.printf("接收消息: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s%n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    
                    // 这里可以添加业务逻辑处理消息
                    // 例如：保存到数据库、调用API、发送邮件等
                }
                
                // 同步提交偏移量
                // commitSync()是阻塞操作，会等待服务器确认提交成功
                // 只有在确认提交成功后才会继续执行后续代码
                // 这确保了偏移量的可靠提交，但会影响性能
                try {
                    consumer.commitSync();
                    System.out.println("偏移量同步提交成功");
                } catch (Exception e) {
                    // 同步提交失败时的异常处理
                    System.err.println("偏移量同步提交失败: " + e.getMessage());
                    
                    // 根据业务需求决定是否重试或采取其他措施
                    // 例如：记录日志、发送告警、重试提交等
                }
            }
        } catch (Exception e) {
            System.err.println("消费过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭消费者，释放资源
            // close()方法会自动提交当前偏移量（如果enable.auto.commit=true）
            // 由于我们禁用了自动提交，建议在关闭前再次同步提交
            try {
                consumer.commitSync();
                System.out.println("消费者关闭前已同步提交偏移量");
            } catch (Exception e) {
                System.err.println("最终同步提交失败: " + e.getMessage());
            }
            
            consumer.close();
            System.out.println("消费者已关闭");
        }
    }
}
