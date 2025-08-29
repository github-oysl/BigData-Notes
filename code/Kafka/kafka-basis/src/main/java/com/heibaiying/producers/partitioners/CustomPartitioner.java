package com.heibaiying.producers.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器示例
 * 
 * 知识点说明：
 * 1. 分区器(Partitioner)决定消息发送到Topic的哪个分区
 * 2. Kafka默认使用DefaultPartitioner，基于消息Key的hash值进行分区
 * 3. 自定义分区器需要实现Partitioner接口的三个方法
 * 4. 分区策略影响消息的分布和消费者的负载均衡
 * 
 * 注意事项：
 * - 分区器的逻辑应该尽可能简单和高效，避免复杂计算
 * - 分区数量一旦确定，不建议随意修改，会影响消息顺序
 * - 相同Key的消息会被分配到同一分区，保证局部有序
 * - 分区器在生产者初始化时创建，在生产者关闭时销毁
 * 
 * @author heibaiying
 */
public class CustomPartitioner implements Partitioner {

    /**
     * 及格线参数，用于分区判断
     * 从生产者配置中获取
     */
    private int passLine;

    /**
     * 分区器配置方法
     * 在生产者创建时调用，用于初始化分区器
     * 
     * @param configs 生产者配置参数Map，包含所有配置项
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // 从配置中获取自定义参数"pass.line"
        // 这个参数在生产者配置中通过props.put("pass.line", 6)设置
        passLine = (Integer) configs.get("pass.line");
        System.out.println("自定义分区器已配置，及格线设置为: " + passLine);
    }

    /**
     * 分区逻辑实现方法
     * 每次发送消息时都会调用此方法来确定目标分区
     * 
     * @param topic Topic名称
     * @param key 消息的Key对象
     * @param keyBytes 消息Key的字节数组形式
     * @param value 消息的Value对象
     * @param valueBytes 消息Value的字节数组形式
     * @param cluster Kafka集群信息，包含Topic和分区的元数据
     * @return 目标分区号（从0开始）
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 自定义分区逻辑：根据Key值与及格线比较
        // 如果Key值大于等于及格线，分配到分区1（及格分区）
        // 如果Key值小于及格线，分配到分区0（不及格分区）
        int targetPartition = (Integer) key >= passLine ? 1 : 0;
        
        System.out.println("消息分区决策: Key=" + key + ", 及格线=" + passLine + ", 目标分区=" + targetPartition);
        
        return targetPartition;
    }

    /**
     * 分区器关闭方法
     * 在生产者关闭时调用，用于清理资源
     */
    @Override
    public void close() {
        System.out.println("自定义分区器关闭，清理资源完成");
    }
}
