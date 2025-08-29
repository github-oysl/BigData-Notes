package com.heibaiying.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

/**
 * Kafka消费者——优雅退出机制示例
 * 
 * 知识点说明：
 * 1. 优雅退出的重要性：
 *    - 确保消费者正确关闭，释放资源
 *    - 避免消费者组重平衡时的延迟
 *    - 保证偏移量正确提交，避免消息丢失或重复
 *    - 防止资源泄漏和连接未正确关闭
 * 
 * 2. wakeup()方法机制：
 *    - 线程安全的方法，可以从其他线程调用
 *    - 会中断consumer.poll()方法的阻塞
 *    - 触发WakeupException异常，用于优雅退出
 *    - 只能使用一次，再次调用会抛出IllegalStateException
 * 
 * 3. 多线程协作模式：
 *    - 主线程：负责消费消息和处理业务逻辑
 *    - 监控线程：监听退出信号，调用wakeup()方法
 *    - 使用Thread.join()等待主线程完成清理工作
 * 
 * 4. 异常处理策略：
 *    - WakeupException：正常的退出信号，不需要特殊处理
 *    - 在finally块中确保消费者正确关闭
 *    - 可以在关闭前进行最后的偏移量提交
 * 
 * 5. 适用场景：
 *    - 长时间运行的消费者应用
 *    - 需要响应外部停止信号的服务
 *    - 生产环境中的消费者服务
 *    - 需要优雅关闭的微服务应用
 * 
 * @author heibaiying
 */
public class ConsumerExit {

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

        // 实现优雅退出机制
        // 获取主线程引用，用于后续的线程同步
        final Thread mainThread = Thread.currentThread();
        
        // 创建监控线程，用于监听退出命令
        Thread shutdownHook = new Thread(() -> {
            System.out.println("启动退出监控线程，输入 'exit' 可优雅退出程序");
            Scanner sc = new Scanner(System.in);
            
            try {
                // 持续监听用户输入
                while (sc.hasNext()) {
                    String input = sc.next();
                    if ("exit".equals(input)) {
                        System.out.println("接收到退出命令，正在优雅关闭消费者...");
                        
                        // 调用wakeup()方法中断消费者的poll()操作
                        // 这是线程安全的方法，可以从其他线程调用
                        consumer.wakeup();
                        
                        try {
                            // 等待主线程完成清理工作（提交偏移量、关闭消费者等）
                            // join()方法确保主线程完全结束后，监控线程才退出
                            mainThread.join();
                            System.out.println("消费者已优雅关闭");
                            break;
                        } catch (InterruptedException e) {
                            System.err.println("等待主线程结束时被中断: " + e.getMessage());
                            Thread.currentThread().interrupt(); // 恢复中断状态
                        }
                    } else {
                        System.out.println("输入 'exit' 退出程序，当前输入: " + input);
                    }
                }
            } finally {
                sc.close();
            }
        });
        
        // 设置为守护线程，确保主程序退出时该线程也会退出
        shutdownHook.setDaemon(true);
        shutdownHook.start();

        try {
            System.out.println("开始消费消息，输入 'exit' 可退出程序");
            
            // 消费消息的主循环
            while (true) {
                // 拉取消息，超时时间100毫秒
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                
                // 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    // 打印消息详细信息
                    System.out.printf("消费消息 - topic=%s, partition=%d, key=%s, value=%s, offset=%d%n",
                            record.topic(), record.partition(), record.key(), record.value(), record.offset());
                    
                    // 这里可以添加具体的业务处理逻辑
                    // 例如：数据处理、存储到数据库、调用其他服务等
                }
                
                // 如果有消息被处理，可以选择提交偏移量
                // 注意：这里为了简化示例，没有实现偏移量提交
                // 在实际应用中，应该根据业务需求决定何时提交偏移量
            }
        } catch (WakeupException e) {
            // 这是正常的退出流程，wakeup()调用会触发此异常
            // 不需要特殊处理，这是预期的行为
            System.out.println("接收到wakeup信号，准备关闭消费者");
        } catch (Exception e) {
            // 处理其他可能的异常
            System.err.println("消费过程中发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                // 在关闭消费者前，可以进行最后的清理工作
                // 例如：提交未提交的偏移量、清理资源等
                System.out.println("正在进行最终清理工作...");
                
                // 如果需要，可以在这里进行最后的偏移量同步提交
                // consumer.commitSync();
                
            } catch (Exception e) {
                System.err.println("清理工作时发生异常: " + e.getMessage());
            } finally {
                // 关闭消费者，释放所有资源
                consumer.close();
                System.out.println("消费者已关闭，程序退出");
            }
        }
    }
}
