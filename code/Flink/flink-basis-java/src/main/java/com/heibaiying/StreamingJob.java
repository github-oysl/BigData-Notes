package com.heibaiying;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink流处理基础示例类
 * 
 * 这是一个简单的Flink流处理应用程序，演示了如何：
 * 1. 创建Flink流执行环境
 * 2. 从文件读取数据创建数据流
 * 3. 将数据流写入到输出文件
 * 4. 执行Flink作业
 * 
 * 主要功能：读取log4j.properties配置文件内容，并将其写入到out文件中
 * 
 * @author heibaiying
 * @version 1.0
 * @since 2023
 */
public class StreamingJob {

    /**
     * 资源文件根路径常量
     * 指向项目的resources目录，用于读取和写入文件
     */
    private static final String ROOT_PATH = "D:\\BigData-Notes\\code\\Flink\\flink-basis-java\\src\\main\\resources\\";

    /**
     * 程序主入口方法
     * 
     * 执行流程：
     * 1. 获取Flink流执行环境
     * 2. 从指定文件创建数据流源
     * 3. 设置数据流的输出目标和并行度
     * 4. 启动执行Flink作业
     * 
     * @param args 命令行参数（本示例中未使用）
     * @throws Exception 当文件读取、写入或作业执行过程中发生异常时抛出
     */
    public static void main(String[] args) throws Exception {

        // 获取Flink流执行环境，这是所有Flink流处理程序的入口点
        // StreamExecutionEnvironment是Flink流处理API的核心类
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 从文本文件创建数据流源
        // readTextFile()方法读取指定路径的文本文件，每行作为一个字符串元素
        // 返回DataStreamSource<String>类型的数据流
        DataStreamSource<String> streamSource = env.readTextFile(ROOT_PATH + "log4j.properties");
        
        // 将数据流写入到输出文件
        // writeAsText()方法将数据流中的每个元素写入到指定的文本文件中
        // setParallelism(1)设置并行度为1，确保输出到单个文件而不是多个分片文件
        streamSource.writeAsText(ROOT_PATH + "out").setParallelism(1);
        
        // 执行Flink作业
        // execute()方法触发作业的实际执行，在此之前所有操作都只是构建执行图
        // 这是一个阻塞调用，会等待作业完成
        env.execute();

    }
}
