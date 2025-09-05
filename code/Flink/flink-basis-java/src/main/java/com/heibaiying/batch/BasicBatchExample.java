package com.heibaiying.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink 批处理基础示例
 * 
 * 功能说明：
 * 1. 从文本文件读取数据
 * 2. 对文本进行单词分割和计数
 * 3. 输出单词统计结果到文件
 * 
 * 适用场景：
 * - 历史数据分析
 * - 离线ETL处理
 * - 数据清洗和转换
 * - 报表生成
 * 
 * @author heibaiying
 */
public class BasicBatchExample {

    /**
     * 程序入口
     * 
     * @param args 命令行参数
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        
        // 1. 获取批处理执行环境
        // ExecutionEnvironment是Flink批处理程序的入口点
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        // 2. 设置并行度为1，便于观察输出结果
        // 在生产环境中，通常会设置更高的并行度以提高处理能力
        env.setParallelism(1);
        
        // 3. 创建数据源 - 从文件读取数据
        // 读取项目resources目录下的sample-data.txt文件
        // 在实际应用中，数据源可能是HDFS、数据库、CSV文件等
        DataSet<String> text = env.readTextFile("src/main/resources/sample-data.txt");
        
        // 4. 数据转换处理
        DataSet<Tuple2<String, Integer>> wordCounts = text
            // 4.1 分割文本为单词
            .flatMap(new WordSplitter())
            // 4.2 按单词分组并计数
            .groupBy(0)  // 按第一个字段（单词）分组
            // 4.3 对每个分组进行求和
            .reduce(new WordCounter());
        
        // 5. 输出结果到文件
        // 将结果写入到output目录
        wordCounts.writeAsText("output/batch-word-count-result");
        
        // 6. 启动执行
        // 批处理程序执行完成后会自动结束
        env.execute("Basic Batch Word Count Example");
        
        System.out.println("批处理任务执行完成！结果已保存到 output/batch-word-count-result 目录");
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
                    // 过滤掉标点符号
                    word = word.replaceAll("[^a-zA-Z0-9]", "");
                    if (word.length() > 0) {
                        out.collect(new Tuple2<>(word, 1));
                    }
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
1. 在src/main/resources/目录下创建sample-data.txt文件，内容如下：
   Apache Flink is a framework and distributed processing engine
   for stateful computations over unbounded and bounded data streams
   Flink has been designed to run in all common cluster environments
   perform computations at in-memory speed and at any scale
   
2. 运行程序

3. 查看output/batch-word-count-result目录下的结果文件

预期输出示例：
(apache,1)
(flink,3)
(is,1)
(a,1)
(framework,1)
(and,3)
(distributed,1)
(processing,1)
(engine,1)
(for,1)
(stateful,1)
(computations,2)
...

学习要点：
- ExecutionEnvironment：批处理环境
- DataSet：批数据集抽象
- readTextFile：从文件读取数据
- groupBy：按字段分组
- writeAsText：写入文本文件
- 批处理vs流处理的区别
*/