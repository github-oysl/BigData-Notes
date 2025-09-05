# Flink Java 基础示例项目

本项目包含了 Apache Flink 在 Java 程序中的各种使用场景示例，适合初学者学习和理解 Flink 的核心概念和功能。

## 项目结构

```
flink-basis-java/
├── src/main/java/com/heibaiying/
│   ├── StreamingJob.java                    # 原始的流处理作业示例
│   ├── streaming/
│   │   └── BasicStreamingExample.java      # 流处理基础示例
│   ├── batch/
│   │   └── BasicBatchExample.java          # 批处理示例
│   ├── window/
│   │   └── WindowOperationExample.java     # 窗口操作示例
│   ├── state/
│   │   └── StatefulProcessingExample.java  # 状态管理示例
│   ├── connector/
│   │   └── KafkaConnectorExample.java      # Kafka连接器示例
│   ├── cep/
│   │   └── ComplexEventProcessingExample.java # CEP复杂事件处理示例
│   └── table/
│       └── TableApiSqlExample.java         # Table API和SQL示例
├── src/main/resources/
│   ├── log4j.properties                    # 日志配置
│   └── sample-data.txt                     # 批处理示例数据
├── pom.xml                                  # Maven配置文件
└── README.md                                # 项目说明文档
```

## 学习路径

建议按照以下顺序学习各个示例：

### 1. 流处理基础 (BasicStreamingExample)
**适用场景**: 实时日志分析、实时监控数据处理、流式ETL处理

**核心概念**:
- StreamExecutionEnvironment：流处理环境
- DataStream：数据流抽象
- FlatMapFunction：一对多转换函数
- ReduceFunction：聚合函数
- keyBy：按键分组

**运行方式**:
1. 启动 netcat：`nc -l 9999`
2. 运行程序
3. 在 netcat 中输入文本观察结果

### 2. 批处理基础 (BasicBatchExample)
**适用场景**: 历史数据分析、离线ETL处理、数据清洗和转换、报表生成

**核心概念**:
- ExecutionEnvironment：批处理环境
- DataSet：批数据集抽象
- groupBy：按字段分组
- writeAsText：写入文本文件

**运行方式**:
1. 确保 `src/main/resources/sample-data.txt` 文件存在
2. 运行程序
3. 查看 `output/batch-word-count-result` 目录下的结果

### 3. 窗口操作 (WindowOperationExample)
**适用场景**: 实时统计分析、滑动窗口监控、异常检测、实时报表生成

**核心概念**:
- TumblingProcessingTimeWindows：滚动时间窗口
- SlidingProcessingTimeWindows：滑动时间窗口
- countWindow：计数窗口
- AggregateFunction：窗口聚合函数

**运行方式**:
1. 直接运行程序
2. 观察不同窗口类型的输出结果

### 4. 状态管理 (StatefulProcessingExample)
**适用场景**: 用户行为分析、异常检测、会话分析、累计统计、去重处理

**核心概念**:
- ValueState：存储单个值的状态
- ListState：存储列表数据的状态
- 状态描述符（StateDescriptor）
- 检查点（Checkpoint）机制

**运行方式**:
1. 直接运行程序
2. 观察用户行为分析结果和异常检测

### 5. Kafka连接器 (KafkaConnectorExample)
**适用场景**: 实时数据管道构建、消息队列数据处理、微服务间数据传输、实时ETL处理

**核心概念**:
- FlinkKafkaConsumer：Kafka数据源
- FlinkKafkaProducer：Kafka数据汇
- 精确一次处理语义
- 偏移量管理

**运行方式**:
1. 启动 Kafka 服务
2. 创建输入和输出主题
3. 运行程序
4. 向输入主题发送消息，观察输出主题的结果

### 6. 复杂事件处理 (ComplexEventProcessingExample)
**适用场景**: 欺诈检测、系统监控、用户行为分析、物联网设备监控、金融风控

**核心概念**:
- Pattern API：定义复杂事件模式
- SimpleCondition：简单条件过滤
- next()：严格连续模式
- within()：时间窗口约束
- PatternSelectFunction：模式匹配结果处理

**运行方式**:
1. 直接运行程序
2. 观察登录失败警报和地点异常警报

### 7. Table API和SQL (TableApiSqlExample)
**适用场景**: 实时数据分析和报表、复杂的聚合计算、多表关联查询、实时OLAP分析

**核心概念**:
- StreamTableEnvironment：Table环境
- fromDataStream：流转表
- toAppendStream：表转追加流
- toRetractStream：表转撤回流
- SQL查询语法

**运行方式**:
1. 直接运行程序
2. 观察各种SQL查询的结果

## 环境要求

- Java 8+
- Maven 3.6+
- Apache Flink 1.9.0
- Apache Kafka 2.x (可选，用于Kafka连接器示例)

## 快速开始

1. **克隆项目**
   ```bash
   git clone <repository-url>
   cd flink-basis-java
   ```

2. **编译项目**
   ```bash
   mvn clean compile
   ```

3. **运行示例**
   ```bash
   # 运行流处理示例
   mvn exec:java -Dexec.mainClass="com.heibaiying.streaming.BasicStreamingExample"
   
   # 运行批处理示例
   mvn exec:java -Dexec.mainClass="com.heibaiying.batch.BasicBatchExample"
   
   # 运行其他示例...
   ```

## 依赖说明

项目包含以下主要依赖：

- **flink-java**: Flink Java API核心库
- **flink-streaming-java**: Flink流处理API
- **flink-connector-kafka**: Kafka连接器
- **flink-cep**: 复杂事件处理库
- **flink-table-api-java-bridge**: Table API桥接库
- **flink-table-planner-blink**: Blink查询优化器
- **slf4j-log4j12**: 日志框架
- **lombok**: Java代码简化工具

## 学习建议

1. **循序渐进**: 按照推荐的学习路径逐个学习示例
2. **动手实践**: 运行每个示例，观察输出结果
3. **修改实验**: 尝试修改代码参数，观察变化
4. **阅读注释**: 每个示例都有详细的中文注释
5. **理解概念**: 重点理解每个示例涉及的核心概念
6. **实际应用**: 思考如何将这些概念应用到实际项目中

## 常见问题

### Q: 如何设置并行度？
A: 使用 `env.setParallelism(n)` 设置全局并行度，或在算子上使用 `.setParallelism(n)` 设置局部并行度。

### Q: 如何处理时间？
A: Flink支持三种时间概念：处理时间（Processing Time）、事件时间（Event Time）和摄入时间（Ingestion Time）。

### Q: 如何保证精确一次处理？
A: 启用检查点机制，使用支持事务的连接器（如Kafka），并正确配置状态后端。

### Q: 如何监控Flink应用？
A: 使用Flink Web UI、Metrics系统，或集成Prometheus等监控工具。

## 进阶学习

完成这些基础示例后，建议继续学习：

1. **Flink集群部署**: Standalone、YARN、Kubernetes
2. **状态后端配置**: MemoryStateBackend、FsStateBackend、RocksDBStateBackend
3. **容错机制**: 检查点、保存点、故障恢复
4. **性能优化**: 并行度调优、内存配置、网络配置
5. **生产环境实践**: 监控、告警、运维

## 参考资料

- [Apache Flink 官方文档](https://flink.apache.org/)
- [Flink 中文社区](https://flink-china.org/)
- [Flink GitHub 仓库](https://github.com/apache/flink)

## 贡献

欢迎提交Issue和Pull Request来改进这个项目！

## 许可证

本项目采用 Apache 2.0 许可证。