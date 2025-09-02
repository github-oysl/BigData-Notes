package com.heibaiying.producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Kafka生产者重试机制常见错误示例
 * 
 * 本类演示了开发中常见的重试机制错误实现，帮助理解为什么这些方法不会正常工作：
 * 1. 异步回调中抛出异常，外部try-catch无法捕获
 * 2. 错误的重试逻辑和异常处理
 * 3. 忽略线程隔离导致的问题
 * 
 * [=重要知识点=]：
 * - 异步执行的回调函数运行在不同的线程中
 * - 外部的try-catch无法捕获其他线程中的异常
 * - 正确的重试应该在回调函数内部处理
 * - 理解同步vs异步的执行模型差异
 * 
 * @author heibaiying
 */
public class ProducerRetryMistakes {

    private final Producer<String, String> producer;

    public ProducerRetryMistakes() {
        this.producer = createProducer();
    }

    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop001:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("retries", 0); // 禁用内置重试，演示手动重试
        return new KafkaProducer<>(props);
    }

    /**
     * 错误示例1：异步回调中抛出异常，外部try-catch无法捕获
     * 
     * [=为什么这种方式不工作=]：
     * 1. send()方法立即返回，不会阻塞
     * 2. 回调函数在Kafka客户端的IO线程中执行
     * 3. 外部的try-catch运行在主线程中
     * 4. 不同线程之间的异常无法跨线程传播
     * 
     * @param topic 主题名称
     * @param key 消息键
     * @param value 消息值
     */
    public void wrongAsyncRetryMethod1(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        int i = 0;
        
        while (i < 3) {
            try {
                System.out.println("[错误示例1] 尝试发送消息，第 " + (i + 1) + " 次");
                
                // 异步发送
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("[错误示例1] 回调中发现异常: " + exception.getMessage());
                            // 在回调中抛出异常
                            throw new RuntimeException("发送失败: " + exception.getMessage());
                        } else {
                            System.out.println("[错误示例1] 发送成功: " + metadata.offset());
                        }
                    }
                });
                
                // 这里会立即执行，不会等待回调完成
                System.out.println("[错误示例1] send()方法已返回，继续执行后续代码");
                break; // 错误：以为发送成功了
                
            } catch (Exception e) {
                // 这个catch永远不会捕获到回调中的异常！
                System.err.println("[错误示例1] 外部catch捕获到异常: " + e.getMessage());
                i++;
            }
        }
        
        System.out.println("[错误示例1] 方法执行完毕，i = " + i);
    }

    /**
     * 错误示例2：错误理解异步执行模型
     * 
     * 这个例子展示了对异步执行的错误理解
     */
    public void wrongAsyncRetryMethod2(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        boolean[] success = {false}; // 尝试用数组来"共享"状态
        Exception[] lastException = {null};
        
        for (int i = 0; i < 3; i++) {
            System.out.println("[错误示例2] 尝试发送消息，第 " + (i + 1) + " 次");
            
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        success[0] = false;
                        lastException[0] = exception;
                        System.err.println("[错误示例2] 发送失败: " + exception.getMessage());
                    } else {
                        success[0] = true;
                        System.out.println("[错误示例2] 发送成功: " + metadata.offset());
                    }
                }
            });
            
            // 错误：立即检查结果，但回调可能还没执行
            if (success[0]) {
                System.out.println("[错误示例2] 检测到成功，退出循环");
                break;
            }
            
            // 错误：短暂等待并不能保证回调执行完成
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        System.out.println("[错误示例2] 最终状态 - 成功: " + success[0] + 
                          ", 异常: " + (lastException[0] != null ? lastException[0].getMessage() : "无"));
    }

    /**
     * 错误示例3：混淆同步和异步的重试逻辑
     * 
     * 这个例子展示了将同步重试逻辑错误地应用到异步场景
     */
    public void wrongAsyncRetryMethod3(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                System.out.println("[错误示例3] 第 " + (attempt + 1) + " 次尝试");
                
                // 异步发送，但错误地期望同步行为
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("[错误示例3] 回调中的异常: " + exception.getMessage());
                            // 错误：试图在回调中控制外部循环
                            // 但这时外部循环可能已经结束了
                        } else {
                            System.out.println("[错误示例3] 回调中的成功: " + metadata.offset());
                        }
                    }
                });
                
                // 错误：认为send()会抛出异常
                System.out.println("[错误示例3] 认为发送成功，退出循环");
                break;
                
            } catch (Exception e) {
                // 这个catch几乎永远不会执行，因为send()本身很少抛出异常
                System.err.println("[错误示例3] 捕获到异常: " + e.getMessage());
                
                if (attempt == 2) {
                    System.err.println("[错误示例3] 达到最大重试次数");
                }
            }
        }
    }

    /**
     * 正确示例：对比展示正确的异步重试方式
     * 
     * [=正确的异步重试原则=]：
     * 1. 重试逻辑必须在回调函数内部实现
     * 2. 使用递归调用或者状态机模式
     * 3. 正确处理线程安全问题
     * 4. 避免无限重试，设置合理的退出条件
     */
    public void correctAsyncRetryMethod(String topic, String key, String value) {
        System.out.println("[正确示例] 开始异步重试发送");
        sendWithRetry(topic, key, value, 0, 3);
    }

    /**
     * 正确的异步重试实现
     * @param topic 主题
     * @param key 键
     * @param value 值
     * @param currentAttempt 当前尝试次数
     * @param maxAttempts 最大尝试次数
     */
    private void sendWithRetry(String topic, String key, String value, int currentAttempt, int maxAttempts) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        System.out.println("[正确示例] 第 " + (currentAttempt + 1) + " 次尝试发送");
        
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("[正确示例] 发送失败: " + exception.getMessage());
                    
                    if (currentAttempt < maxAttempts - 1) {
                        // 在回调内部进行重试
                        System.out.println("[正确示例] 准备重试...");
                        
                        // 延迟重试（可选）
                        new Thread(() -> {
                            try {
                                Thread.sleep(1000 * (currentAttempt + 1)); // 指数退避
                                sendWithRetry(topic, key, value, currentAttempt + 1, maxAttempts);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                System.err.println("[正确示例] 重试被中断");
                            }
                        }).start();
                    } else {
                        System.err.println("[正确示例] 达到最大重试次数，最终失败");
                    }
                } else {
                    System.out.println("[正确示例] 发送成功: topic=" + metadata.topic() + 
                                     ", partition=" + metadata.partition() + 
                                     ", offset=" + metadata.offset() + 
                                     ", 尝试次数=" + (currentAttempt + 1));
                }
            }
        });
    }

    /**
     * 演示同步发送的正确重试方式（对比参考）
     */
    public void correctSyncRetryMethod(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                System.out.println("[同步正确示例] 第 " + (attempt + 1) + " 次尝试");
                
                // 同步发送，会阻塞直到完成或异常
                RecordMetadata metadata = producer.send(record).get();
                
                System.out.println("[同步正确示例] 发送成功: " + metadata.offset());
                return; // 成功后退出
                
            } catch (ExecutionException e) {
                System.err.println("[同步正确示例] 发送失败: " + e.getCause().getMessage());
                
                if (attempt == 2) {
                    System.err.println("[同步正确示例] 达到最大重试次数，最终失败");
                } else {
                    try {
                        Thread.sleep(1000); // 重试间隔
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("[同步正确示例] 发送被中断");
                return;
            }
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    /**
     * 主方法：演示各种错误和正确的重试方式
     */
    public static void main(String[] args) {
        ProducerRetryMistakes demo = new ProducerRetryMistakes();
        String topic = "Hello-Kafka";
        
        try {
            System.out.println("========== 错误示例演示 ==========");
            
            // 错误示例1
            System.out.println("\n--- 错误示例1：异步回调异常无法被外部捕获 ---");
            demo.wrongAsyncRetryMethod1(topic, "wrong1", "value1");
            Thread.sleep(2000); // 等待回调执行
            
            // 错误示例2
            System.out.println("\n--- 错误示例2：错误理解异步执行模型 ---");
            demo.wrongAsyncRetryMethod2(topic, "wrong2", "value2");
            Thread.sleep(2000);
            
            // 错误示例3
            System.out.println("\n--- 错误示例3：混淆同步和异步重试逻辑 ---");
            demo.wrongAsyncRetryMethod3(topic, "wrong3", "value3");
            Thread.sleep(2000);
            
            System.out.println("\n========== 正确示例演示 ==========");
            
            // 正确的异步重试
            System.out.println("\n--- 正确示例：异步重试 ---");
            demo.correctAsyncRetryMethod(topic, "correct-async", "async-value");
            Thread.sleep(5000);
            
            // 正确的同步重试
            System.out.println("\n--- 正确示例：同步重试 ---");
            demo.correctSyncRetryMethod(topic, "correct-sync", "sync-value");
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            demo.close();
        }
    }
}