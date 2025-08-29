package com.heibaiying.producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Kafka生产者异步发送示例
 * 
 * 知识点说明：
 * 1. 异步发送：producer.send()方法本身就是异步的，不会阻塞等待结果
 * 2. 回调机制：通过Callback接口可以在消息发送完成后执行自定义逻辑
 * 3. RecordMetadata：包含消息发送后的元数据信息（topic、partition、offset等）
 * 4. 异常处理：可以在回调中处理发送失败的情况
 * 
 * 注意事项：
 * - 回调函数在生产者的I/O线程中执行，不要在回调中执行耗时操作
 * - 回调函数的执行顺序可能与发送顺序不同
 * - 如果不需要处理发送结果，可以不提供回调函数
 * - 异步发送可以提高吞吐量，但需要合理处理异常情况
 * 
 * @author heibaiying
 */
public class ProducerASyn {

    /**
     * 主方法：演示异步发送消息并处理回调
     * @param args 命令行参数
     */
    public static void main(String[] args) {

        // 定义目标Topic
        String topicName = "Hello-Kafka";

        // 创建生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop001:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // 创建生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 循环发送10条消息
        for (int i = 0; i < 10; i++) {
            // 创建消息记录
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "k" + i, "world" + i);
            
            // 异步发送消息，并提供回调函数
            // send()方法的第二个参数是Callback接口的实现
            producer.send(record, new Callback() {
                /**
                 * 消息发送完成后的回调方法
                 * @param metadata 消息发送成功后的元数据信息，包含topic、partition、offset等
                 * @param exception 发送过程中的异常，如果发送成功则为null
                 */
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        // 发送失败，进行异常处理
                        System.err.println("消息发送失败: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        // 发送成功，打印消息的元数据信息
                        // topic: 消息所属的主题
                        // partition: 消息被分配到的分区号
                        // offset: 消息在分区中的偏移量（唯一标识）
                        System.out.printf("消息发送成功 -> topic=%s, partition=%d, offset=%s \n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                }
            });
            
            System.out.println("已提交发送请求: Key=" + "k" + i + ", Value=" + "world" + i);
        }

        // 关闭生产者
        // close()方法会等待所有未完成的发送请求完成
        // 包括等待所有回调函数执行完毕
        producer.close();
        
        System.out.println("生产者已关闭");
    }
}