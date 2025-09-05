package com.heibaiying.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

/**
 * Flink 窗口操作示例
 * 
 * 功能说明：
 * 1. 演示时间窗口（滚动窗口、滑动窗口）
 * 2. 演示计数窗口
 * 3. 展示窗口聚合操作
 * 
 * 适用场景：
 * - 实时统计分析（每分钟订单量、每小时访问量等）
 * - 滑动窗口监控（最近5分钟的平均响应时间）
 * - 异常检测（基于窗口的阈值监控）
 * - 实时报表生成
 * 
 * @author heibaiying
 */
public class WindowOperationExample {

    /**
     * 程序入口
     * 
     * @param args 命令行参数
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        
        // 1. 获取流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2. 创建模拟数据源 - 生成传感器数据
        DataStream<SensorReading> sensorData = env.addSource(new SensorDataSource());
        
        // 3. 转换为键值对格式 (传感器ID, 温度值)
        DataStream<Tuple2<String, Double>> sensorTuples = sensorData
            .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> map(SensorReading value) {
                    return new Tuple2<>(value.sensorId, value.temperature);
                }
            });
        
        // 4. 滚动时间窗口示例 - 每10秒统计一次平均温度
        System.out.println("=== 滚动时间窗口示例（每10秒统计平均温度） ===");
        DataStream<Tuple2<String, Double>> tumblingWindowResult = sensorTuples
            .keyBy(value -> value.f0)  // 按传感器ID分组
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))  // 10秒滚动窗口
            .aggregate(new TemperatureAverageAggregator());  // 计算平均值
        
        tumblingWindowResult.print("滚动窗口结果");
        
        // 5. 滑动时间窗口示例 - 每5秒计算最近15秒的平均温度
        System.out.println("=== 滑动时间窗口示例（每5秒计算最近15秒平均温度） ===");
        DataStream<Tuple2<String, Double>> slidingWindowResult = sensorTuples
            .keyBy(value -> value.f0)  // 按传感器ID分组
            .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)))  // 15秒窗口，5秒滑动
            .aggregate(new TemperatureAverageAggregator());  // 计算平均值
        
        slidingWindowResult.print("滑动窗口结果");
        
        // 6. 计数窗口示例 - 每收集到5个数据点就计算一次平均值
        System.out.println("=== 计数窗口示例（每5个数据点计算平均温度） ===");
        DataStream<Tuple2<String, Double>> countWindowResult = sensorTuples
            .keyBy(value -> value.f0)  // 按传感器ID分组
            .countWindow(5)  // 计数窗口，每5个元素触发一次
            .aggregate(new TemperatureAverageAggregator());  // 计算平均值
        
        countWindowResult.print("计数窗口结果");
        
        // 7. 启动执行
        env.execute("Window Operation Example");
    }
    
    /**
     * 传感器读数数据类
     */
    public static class SensorReading {
        public String sensorId;      // 传感器ID
        public long timestamp;       // 时间戳
        public double temperature;   // 温度值
        
        public SensorReading() {}
        
        public SensorReading(String sensorId, long timestamp, double temperature) {
            this.sensorId = sensorId;
            this.timestamp = timestamp;
            this.temperature = temperature;
        }
        
        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", timestamp=" + timestamp +
                    ", temperature=" + temperature +
                    '}';
        }
    }
    
    /**
     * 模拟传感器数据源
     * 生成随机的传感器温度数据
     */
    public static class SensorDataSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private Random random = new Random();
        private String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3"};
        
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                // 为每个传感器生成随机温度数据
                for (String sensorId : sensorIds) {
                    double temperature = 15 + random.nextGaussian() * 10; // 平均15度，标准差10度
                    SensorReading reading = new SensorReading(
                        sensorId, 
                        System.currentTimeMillis(), 
                        Math.round(temperature * 100.0) / 100.0  // 保留两位小数
                    );
                    ctx.collect(reading);
                }
                
                // 每2秒生成一批数据
                Thread.sleep(2000);
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
    
    /**
     * 温度平均值聚合器
     * 实现AggregateFunction接口，计算窗口内温度的平均值
     */
    public static class TemperatureAverageAggregator 
            implements AggregateFunction<Tuple2<String, Double>, Tuple2<Double, Integer>, Tuple2<String, Double>> {
        
        /**
         * 创建累加器
         * @return 初始累加器 (总和, 计数)
         */
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }
        
        /**
         * 添加元素到累加器
         * @param value 输入值
         * @param accumulator 累加器
         * @return 更新后的累加器
         */
        @Override
        public Tuple2<Double, Integer> add(Tuple2<String, Double> value, 
                                          Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1);
        }
        
        /**
         * 获取最终结果
         * @param accumulator 累加器
         * @return 平均值结果
         */
        @Override
        public Tuple2<String, Double> getResult(Tuple2<Double, Integer> accumulator) {
            double average = accumulator.f0 / accumulator.f1;
            return new Tuple2<>("average", Math.round(average * 100.0) / 100.0);
        }
        
        /**
         * 合并两个累加器（用于并行处理）
         * @param a 累加器A
         * @param b 累加器B
         * @return 合并后的累加器
         */
        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, 
                                           Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}

/*
使用说明：
1. 直接运行程序，会自动生成模拟的传感器数据

2. 观察控制台输出，会看到三种不同窗口的结果：
   - 滚动窗口：每10秒输出一次结果
   - 滑动窗口：每5秒输出一次最近15秒的结果
   - 计数窗口：每收集5个数据点输出一次结果

3. 输出示例：
   滚动窗口结果> (average,16.23)
   滑动窗口结果> (average,15.87)
   计数窗口结果> (average,14.56)

学习要点：
- TumblingProcessingTimeWindows：滚动时间窗口
- SlidingProcessingTimeWindows：滑动时间窗口
- countWindow：计数窗口
- AggregateFunction：窗口聚合函数
- 窗口触发机制
- 处理时间vs事件时间
- 窗口的生命周期

窗口类型对比：
1. 滚动窗口：不重叠，每个元素只属于一个窗口
2. 滑动窗口：可重叠，元素可能属于多个窗口
3. 计数窗口：基于元素数量而非时间
4. 会话窗口：基于活动间隔（本例未演示）
*/