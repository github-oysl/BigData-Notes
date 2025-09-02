package com.heibaiying.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka生产者重试机制示例
 * 
 * 本类演示了多种重试策略的实现方式：
 * 1. 同步发送 + 手动重试
 * 2. 异步发送 + 回调内重试
 * 3. 使用CompletableFuture的重试
 * 4. Kafka内置重试配置
 * 
 * 知识点说明：
 * - 重试机制的必要性：网络抖动、Broker临时不可用等情况
 * - 不同重试策略的适用场景和性能影响
 * - 重试间隔和退避策略的重要性
 * - 如何避免无限重试导致的资源浪费
 * 
 * @author heibaiying
 */
public class ProducerWithRetry {

    private final Producer<String, String> producer;
    private final int maxRetries;
    private final long retryDelayMs;

    /**
     * 构造函数：初始化生产者和重试参数
     * @param maxRetries 最大重试次数
     * @param retryDelayMs 重试间隔（毫秒）
     */
    public ProducerWithRetry(int maxRetries, long retryDelayMs) {
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        this.producer = createProducer();
    }

    /**
     * 创建Kafka生产者实例
     * @return 配置好的生产者实例
     */
    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop001:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // Kafka内置重试配置
        props.put("retries", 3); // 内置重试次数
        props.put("retry.backoff.ms", 1000); // 重试间隔
        props.put("delivery.timeout.ms", 30000); // 总超时时间
        props.put("request.timeout.ms", 10000); // 单次请求超时
        props.put("max.in.flight.requests.per.connection", 1); // 保证消息顺序
        
        return new KafkaProducer<>(props);
    }

    /**
     * 方式一：同步发送 + 手动重试
     * 优点：逻辑清晰，易于理解和调试
     * 缺点：阻塞主线程，性能较低
     * 
     * @param topic 主题名称
     * @param key 消息键
     * @param value 消息值
     * @return 是否发送成功
     */
    public boolean sendSyncWithRetry(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                // 同步发送消息
                RecordMetadata metadata = producer.send(record).get(10, TimeUnit.SECONDS);
                System.out.printf("[同步重试] 发送成功 -> topic=%s, partition=%d, offset=%d, 尝试次数=%d\n",
                        metadata.topic(), metadata.partition(), metadata.offset(), attempt + 1);
                return true;
                
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                
                // 判断是否为可重试异常
                if (isRetriableException(cause) && attempt < maxRetries) {
                    System.err.printf("[同步重试] 发送失败，第 %d 次重试: %s\n", attempt + 1, cause.getMessage());
                    
                    // 等待重试间隔
                    try {
                        Thread.sleep(retryDelayMs * (attempt + 1)); // 指数退避
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                } else {
                    System.err.printf("[同步重试] 不可重试异常或达到最大重试次数: %s\n", cause.getMessage());
                    return false;
                }
                
            } catch (InterruptedException | TimeoutException e) {
                System.err.printf("[同步重试] 发送超时或被中断: %s\n", e.getMessage());
                if (attempt < maxRetries) {
                    continue;
                } else {
                    return false;
                }
            }
        }
        
        return false;
    }

    /**
     * 方式二：异步发送 + 回调内重试
     * 优点：不阻塞主线程，性能较高
     * 缺点：逻辑相对复杂，需要处理递归调用
     * 
     * @param topic 主题名称
     * @param key 消息键
     * @param value 消息值
     * @param callback 最终结果回调
     */
    public void sendAsyncWithRetry(String topic, String key, String value, 
                                   final ResultCallback callback) {
        sendAsyncWithRetry(topic, key, value, 0, callback);
    }

    /**
     * 异步重试的内部实现方法
     * @param topic 主题名称
     * @param key 消息键
     * @param value 消息值
     * @param currentAttempt 当前尝试次数
     * @param callback 最终结果回调
     */
    private void sendAsyncWithRetry(String topic, String key, String value, 
                                    int currentAttempt, final ResultCallback callback) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    // 发送成功
                    System.out.printf("[异步重试] 发送成功 -> topic=%s, partition=%d, offset=%d, 尝试次数=%d\n",
                            metadata.topic(), metadata.partition(), metadata.offset(), currentAttempt + 1);
                    callback.onSuccess(metadata);
                    
                } else if (isRetriableException(exception) && currentAttempt < maxRetries) {
                    // 可重试异常，进行重试
                    System.err.printf("[异步重试] 发送失败，第 %d 次重试: %s\n", 
                            currentAttempt + 1, exception.getMessage());
                    
                    // 延迟重试（使用线程池或定时器）
                    new Thread(() -> {
                        try {
                            Thread.sleep(retryDelayMs * (currentAttempt + 1));
                            sendAsyncWithRetry(topic, key, value, currentAttempt + 1, callback);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            callback.onFailure(e);
                        }
                    }).start();
                    
                } else {
                    // 不可重试异常或达到最大重试次数
                    System.err.printf("[异步重试] 最终失败: %s\n", exception.getMessage());
                    callback.onFailure(exception);
                }
            }
        });
    }

    /**
     * 方式三：使用CompletableFuture的重试机制
     * 优点：现代化的异步编程模式，支持链式调用
     * 缺点：需要Java 8+，学习成本相对较高
     * 
     * @param topic 主题名称
     * @param key 消息键
     * @param value 消息值
     * @return CompletableFuture包装的结果
     */
    public CompletableFuture<RecordMetadata> sendWithCompletableFuture(String topic, String key, String value) {
        return sendWithCompletableFutureInternal(topic, key, value, 0);
    }

    /**
     * CompletableFuture重试的内部实现
     * @param topic 主题名称
     * @param key 消息键
     * @param value 消息值
     * @param currentAttempt 当前尝试次数
     * @return CompletableFuture包装的结果
     */
    private CompletableFuture<RecordMetadata> sendWithCompletableFutureInternal(
            String topic, String key, String value, int currentAttempt) {
        
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("[CompletableFuture] 发送成功 -> topic=%s, partition=%d, offset=%d, 尝试次数=%d\n",
                        metadata.topic(), metadata.partition(), metadata.offset(), currentAttempt + 1);
                future.complete(metadata);
                
            } else if (isRetriableException(exception) && currentAttempt < maxRetries) {
                System.err.printf("[CompletableFuture] 发送失败，第 %d 次重试: %s\n", 
                        currentAttempt + 1, exception.getMessage());
                
                // 延迟重试
                CompletableFuture.delayedExecutor(retryDelayMs * (currentAttempt + 1), TimeUnit.MILLISECONDS)
                    .execute(() -> {
                        sendWithCompletableFutureInternal(topic, key, value, currentAttempt + 1)
                            .whenComplete((result, ex) -> {
                                if (ex != null) {
                                    future.completeExceptionally(ex);
                                } else {
                                    future.complete(result);
                                }
                            });
                    });
                    
            } else {
                System.err.printf("[CompletableFuture] 最终失败: %s\n", exception.getMessage());
                future.completeExceptionally(exception);
            }
        });
        
        return future;
    }

    /**
     * 判断异常是否可重试
     * @param exception 异常对象
     * @return 是否可重试
     */
    private boolean isRetriableException(Throwable exception) {
        // Kafka的可重试异常通常实现了RetriableException接口
        if (exception instanceof RetriableException) {
            return true;
        }
        
        // 也可以根据具体的异常类型判断
        String exceptionName = exception.getClass().getSimpleName();
        return exceptionName.contains("Timeout") || 
               exceptionName.contains("Disconnect") ||
               exceptionName.contains("NotLeader") ||
               exceptionName.contains("NetworkException");
    }

    /**
     * 关闭生产者
     */
    public void close() {
        if (producer != null) {
            producer.close();
            System.out.println("生产者已关闭");
        }
    }

    /**
     * 结果回调接口
     */
    public interface ResultCallback {
        void onSuccess(RecordMetadata metadata);
        void onFailure(Exception exception);
    }

    /**
     * 主方法：演示各种重试机制
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        ProducerWithRetry producer = new ProducerWithRetry(3, 1000);
        String topic = "Hello-Kafka";
        
        try {
            // 演示同步重试
            System.out.println("=== 演示同步重试 ===");
            boolean syncResult = producer.sendSyncWithRetry(topic, "sync-key", "sync-value");
            System.out.println("同步发送结果: " + syncResult);
            
            // 演示异步重试
            System.out.println("\n=== 演示异步重试 ===");
            producer.sendAsyncWithRetry(topic, "async-key", "async-value", new ResultCallback() {
                @Override
                public void onSuccess(RecordMetadata metadata) {
                    System.out.println("异步发送成功: " + metadata.offset());
                }
                
                @Override
                public void onFailure(Exception exception) {
                    System.err.println("异步发送失败: " + exception.getMessage());
                }
            });
            
            // 演示CompletableFuture重试
            System.out.println("\n=== 演示CompletableFuture重试 ===");
            producer.sendWithCompletableFuture(topic, "future-key", "future-value")
                .thenAccept(metadata -> System.out.println("CompletableFuture发送成功: " + metadata.offset()))
                .exceptionally(throwable -> {
                    System.err.println("CompletableFuture发送失败: " + throwable.getMessage());
                    return null;
                });
            
            // 等待异步操作完成
            Thread.sleep(10000);
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}