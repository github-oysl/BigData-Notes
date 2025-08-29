package com.heibaiying.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Kafka生产者基础示例
 * 
 * 知识点说明：
 * 1. Kafka Producer是向Kafka集群发送消息的客户端
 * 2. 生产者将消息发送到指定的Topic中
 * 3. 消息由Key和Value组成，Key用于分区路由，Value是实际的消息内容
 * 4. 生产者需要配置序列化器来将Java对象转换为字节数组
 * 
 * 注意事项：
 * - 生产者是线程安全的，可以在多线程环境中使用
 * - 使用完毕后必须调用close()方法释放资源
 * - bootstrap.servers配置应该包含多个broker地址以提高可用性
 * 
 * @author heibaiying
 */
public class SimpleProducer {

    /**
     * 主方法：演示最基本的Kafka生产者使用方式
     * @param args 命令行参数
     */
    public static void main(String[] args) {

        // 定义要发送消息的Topic名称
        // Topic是Kafka中消息的逻辑分类，类似于数据库中的表
        String topicName = "Hello-Kafka";

        // 创建生产者配置属性
        Properties props = new Properties();
        
        // 配置Kafka集群地址（bootstrap.servers）
        // 这是生产者连接Kafka集群的入口点，可以配置多个broker地址用逗号分隔
        // 格式：host1:port1,host2:port2,host3:port3
        props.put("bootstrap.servers", "hadoop001:9092");
        
        // 配置Key序列化器（key.serializer）
        // 将消息的Key从Java对象序列化为字节数组
        // StringSerializer用于处理字符串类型的Key
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // 配置Value序列化器（value.serializer）
        // 将消息的Value从Java对象序列化为字节数组
        // StringSerializer用于处理字符串类型的Value
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // 创建Kafka生产者实例
        // Producer<K,V>中K是Key的类型，V是Value的类型
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 循环发送10条消息
        for (int i = 0; i < 10; i++) {
            // 创建ProducerRecord对象
            // ProducerRecord包含：Topic名称、消息Key、消息Value
            // Key用于确定消息发送到哪个分区，相同Key的消息会发送到同一分区
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello" + i, "world" + i);
            
            // 发送消息（异步发送）
            // send()方法是异步的，不会阻塞等待发送结果
            // 消息会先存储在生产者的内存缓冲区中，然后批量发送到Kafka
            producer.send(record);
            
            System.out.println("发送消息: Key=" + "hello" + i + ", Value=" + "world" + i);
        }

        // 关闭生产者，释放资源
        // close()方法会等待所有未完成的发送请求完成，然后关闭连接
        // 这是一个阻塞操作，确保所有消息都已发送
        producer.close();
        
        System.out.println("所有消息发送完成，生产者已关闭");
    }
}