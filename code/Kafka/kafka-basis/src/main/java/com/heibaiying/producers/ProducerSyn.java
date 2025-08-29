package com.heibaiying.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka生产者示例——同步发送消息
 * 
 * 知识点说明：
 * 1. 同步发送 vs 异步发送
 *    - 同步发送：调用send()方法后立即调用get()方法，会阻塞等待服务器响应
 *    - 异步发送：调用send()方法后不等待响应，通过回调函数处理结果
 * 
 * 2. 同步发送的特点：
 *    - 可靠性高：能够确保消息发送成功或失败
 *    - 性能较低：每条消息都要等待服务器响应，吞吐量受限
 *    - 简单易用：错误处理直观，适合对可靠性要求高的场景
 * 
 * 3. 适用场景：
 *    - 对消息可靠性要求极高的业务
 *    - 需要确保消息顺序的场景
 *    - 发送频率不高的重要消息
 * 
 * 4. 注意事项：
 *    - get()方法会抛出InterruptedException和ExecutionException
 *    - 同步发送会显著降低吞吐量
 *    - 建议在生产环境中谨慎使用，优先考虑异步发送
 * 
 * @author heibaiying
 */
public class ProducerSyn {

    public static void main(String[] args) {

        // 指定要发送消息的主题名称
        String topicName = "Hello-Kafka";

        // 配置生产者属性
        Properties props = new Properties();
        
        // Kafka集群地址，多个地址用逗号分隔
        // 注意：根据实际环境修改为正确的Kafka服务器地址
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 键的序列化器：将键对象转换为字节数组
        // StringSerializer用于处理字符串类型的键
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // 值的序列化器：将值对象转换为字节数组
        // StringSerializer用于处理字符串类型的值
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // 创建Kafka生产者实例
        // Producer是线程安全的，可以在多个线程中共享使用
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 循环发送10条消息
        for (int i = 0; i < 10; i++) {
            try {
                // 创建消息记录
                // 参数：主题名称、消息键、消息值
                // 消息键用于分区选择，相同键的消息会发送到同一分区
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "k" + i, "world" + i);
                
                // 同步发送消息
                // send()方法返回Future<RecordMetadata>对象
                // 调用get()方法会阻塞当前线程，直到消息发送完成或失败
                RecordMetadata metadata = producer.send(record).get();
                
                // 打印消息发送结果
                // metadata包含消息的元数据信息：主题、分区、偏移量等
                System.out.printf("同步发送成功 - topic=%s, partition=%d, offset=%s \n",
                        metadata.topic(), metadata.partition(), metadata.offset());
                        
            } catch (InterruptedException e) {
                // 线程中断异常处理
                System.err.println("发送消息时线程被中断: " + e.getMessage());
                // 恢复中断状态
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                // 执行异常处理（包括网络异常、序列化异常等）
                System.err.println("消息发送失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // 关闭生产者，释放资源
        // close()方法会等待所有未完成的请求完成，然后关闭连接
        // 注意：在应用程序退出前必须调用此方法，否则可能导致资源泄露
        producer.close();
        
        System.out.println("同步生产者示例执行完成");
    }
}