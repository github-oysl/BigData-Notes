package com.heibaiying.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink 流处理基础示例
 * 
 * 功能说明：
 * 1. 从Socket接收实时文本数据
 * 2. 对文本进行单词分割和计数
 * 3. 实时输出单词统计结果
 * 
 * 适用场景：
 * - 实时日志分析
 * - 实时监控数据处理
 * - 流式ETL处理
 * 
 * @author heibaiying
 */
public class BasicStreamingExample {

    /**
     * 程序入口
     * 
     * @param args 命令行参数
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        
        // 1. 获取流处理执行环境
        // StreamExecutionEnvironment是Flink流处理程序的入口点
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 设置并行度为1，便于观察输出结果
        // 在生产环境中，通常会设置更高的并行度以提高处理能力
        env.setParallelism(1);
        
        // 3. 创建数据源 - 从Socket接收数据
        // 连接到localhost:9999端口，接收文本数据
        // 在实际应用中，数据源可能是Kafka、文件系统、数据库等
        DataStream<String> text = env.socketTextStream("localhost", 9999);
        
        // 4. 数据转换处理
        DataStream<Tuple2<String, Integer>> wordCounts = text
            // 4.1 分割文本为单词
            .flatMap(new WordSplitter())
            // 4.2 按单词分组
            .keyBy(value -> value.f0)
            // 4.3 对每个单词进行计数累加
            .reduce(new WordCounter());
        
        // 5. 输出结果到控制台
        // 在生产环境中，通常会输出到Kafka、数据库、文件系统等
        wordCounts.print();
        
        // 6. 启动执行
        // Flink程序是懒加载的，只有调用execute()才会真正开始执行
        env.execute("Basic Streaming Word Count Example");
    }
    
    /**
     * 单词分割器
     * 实现FlatMapFunction接口，将输入的文本行分割为单词
     */
    public static class WordSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        
        /**
         * 分割文本为单词
         * 
         * @param value 输入的文本行
         * @param out 输出收集器
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 将文本转为小写并按空格分割
            String[] words = value.toLowerCase().split("\\s+");
            
            // 为每个单词创建一个(单词, 1)的元组
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
    
    /**
     * 单词计数器
     * 实现ReduceFunction接口，对相同单词的计数进行累加
     */
    public static class WordCounter implements ReduceFunction<Tuple2<String, Integer>> {
        
        /**
         * 累加计数
         * 
         * @param value1 第一个值
         * @param value2 第二个值
         * @return 累加后的结果
         */
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, 
                                            Tuple2<String, Integer> value2) {
            // 将两个相同单词的计数相加
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }
}

/*
使用说明：
1. 启动程序前，需要先在命令行启动netcat服务：
   Windows: 下载netcat工具，运行 nc -l -p 9999
   Linux/Mac: nc -l 9999
   
2. 启动Flink程序

3. 在netcat窗口中输入文本，如：
   hello world
   hello flink
   world peace
   
4. 观察控制台输出的实时单词计数结果：
   (hello,1)
   (world,1)
   (hello,2)
   (flink,1)
   (world,2)
   (peace,1)

学习要点：
- StreamExecutionEnvironment：流处理环境
- DataStream：数据流抽象
- FlatMapFunction：一对多转换函数
- ReduceFunction：聚合函数
- keyBy：按键分组
- 懒加载执行模式
*/