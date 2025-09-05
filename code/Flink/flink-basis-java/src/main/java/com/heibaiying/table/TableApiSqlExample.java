package com.heibaiying.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * Flink Table API 和 SQL 示例
 * 
 * 功能说明：
 * 1. 演示Table API的基本使用
 * 2. 展示SQL查询功能
 * 3. 实现流表转换
 * 4. 展示窗口聚合查询
 * 
 * 适用场景：
 * - 实时数据分析和报表
 * - 复杂的聚合计算
 * - 多表关联查询
 * - 实时OLAP分析
 * - 数据仓库实时更新
 * 
 * 注意：此示例需要Table API依赖，需要在pom.xml中添加相应依赖
 * 
 * @author heibaiying
 */
public class TableApiSqlExample {

    /**
     * 程序入口
     * 
     * @param args 命令行参数
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        
        // 1. 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 2. 创建Table环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()  // 使用Blink Planner
            .inStreamingMode()  // 流处理模式
            .build();
        
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        // 3. 创建模拟订单数据源
        DataStream<Order> orderStream = env.addSource(new OrderSource());
        
        // 4. 将DataStream转换为Table
        Table orderTable = tableEnv.fromDataStream(orderStream, 
            "orderId, userId, productName, price, orderTime.rowtime");
        
        // 注册表，以便在SQL中使用
        tableEnv.createTemporaryView("Orders", orderTable);
        
        // 5. Table API 示例 - 基本查询和过滤
        System.out.println("=== Table API 基本查询示例 ===");
        Table highValueOrders = orderTable
            .filter("price > 100")  // 过滤高价值订单
            .select("orderId, userId, productName, price");  // 选择字段
        
        // 转换回DataStream并输出
        DataStream<Row> highValueResult = tableEnv.toAppendStream(highValueOrders, Row.class);
        highValueResult.print("高价值订单");
        
        // 6. SQL 查询示例 - 用户订单统计
        System.out.println("=== SQL 查询示例 ===");
        Table userOrderStats = tableEnv.sqlQuery(
            "SELECT userId, " +
            "       COUNT(*) as orderCount, " +
            "       SUM(price) as totalAmount, " +
            "       AVG(price) as avgPrice " +
            "FROM Orders " +
            "GROUP BY userId"
        );
        
        // 转换为更新流（因为是聚合查询）
        DataStream<Row> userStatsResult = tableEnv.toRetractStream(userOrderStats, Row.class)
            .filter(tuple -> tuple.f0)  // 只保留插入记录，过滤删除记录
            .map(tuple -> tuple.f1);    // 提取Row数据
        
        userStatsResult.print("用户订单统计");
        
        // 7. 时间窗口聚合查询
        System.out.println("=== 时间窗口聚合查询 ===");
        Table windowAggResult = tableEnv.sqlQuery(
            "SELECT " +
            "  TUMBLE_START(orderTime, INTERVAL '10' SECOND) as windowStart, " +
            "  TUMBLE_END(orderTime, INTERVAL '10' SECOND) as windowEnd, " +
            "  COUNT(*) as orderCount, " +
            "  SUM(price) as totalRevenue " +
            "FROM Orders " +
            "GROUP BY TUMBLE(orderTime, INTERVAL '10' SECOND)"
        );
        
        DataStream<Row> windowResult = tableEnv.toAppendStream(windowAggResult, Row.class);
        windowResult.print("10秒窗口统计");
        
        // 8. 复杂SQL查询 - Top N 产品
        Table topProductsTable = tableEnv.sqlQuery(
            "SELECT productName, " +
            "       SUM(price) as totalSales, " +
            "       COUNT(*) as salesCount " +
            "FROM Orders " +
            "GROUP BY productName " +
            "HAVING COUNT(*) >= 2"  // 至少销售2次的产品
        );
        
        DataStream<Row> topProductsResult = tableEnv.toRetractStream(topProductsTable, Row.class)
            .filter(tuple -> tuple.f0)
            .map(tuple -> tuple.f1);
        
        topProductsResult.print("热销产品");
        
        // 9. Table API 链式操作示例
        Table chainedResult = orderTable
            .filter("price > 50")                    // 过滤价格大于50的订单
            .groupBy("productName")                  // 按产品分组
            .select("productName, " +                // 选择字段并聚合
                   "price.sum as totalSales, " +
                   "price.avg as avgPrice, " +
                   "price.count as salesCount");
        
        DataStream<Row> chainedResultStream = tableEnv.toRetractStream(chainedResult, Row.class)
            .filter(tuple -> tuple.f0)
            .map(tuple -> tuple.f1);
        
        chainedResultStream.print("产品销售分析");
        
        // 10. 启动执行
        env.execute("Table API and SQL Example");
    }
    
    /**
     * 订单数据类
     */
    public static class Order {
        public String orderId;       // 订单ID
        public String userId;        // 用户ID
        public String productName;   // 产品名称
        public Double price;         // 价格
        public Long orderTime;       // 订单时间
        
        public Order() {}
        
        public Order(String orderId, String userId, String productName, Double price, Long orderTime) {
            this.orderId = orderId;
            this.userId = userId;
            this.productName = productName;
            this.price = price;
            this.orderTime = orderTime;
        }
        
        @Override
        public String toString() {
            return String.format("Order{orderId='%s', userId='%s', productName='%s', price=%.2f, orderTime=%d}",
                orderId, userId, productName, price, orderTime);
        }
    }
    
    /**
     * 模拟订单数据源
     */
    public static class OrderSource implements SourceFunction<Order> {
        private boolean running = true;
        private Random random = new Random();
        private String[] userIds = {"user_001", "user_002", "user_003", "user_004", "user_005"};
        private String[] products = {"iPhone", "iPad", "MacBook", "AirPods", "Apple Watch", "iMac"};
        private int orderCounter = 1;
        
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while (running) {
                // 生成随机订单数据
                String orderId = "order_" + String.format("%06d", orderCounter++);
                String userId = userIds[random.nextInt(userIds.length)];
                String productName = products[random.nextInt(products.length)];
                
                // 根据产品类型设置价格范围
                double price;
                switch (productName) {
                    case "iPhone":
                        price = 800 + random.nextDouble() * 400;  // 800-1200
                        break;
                    case "iPad":
                        price = 300 + random.nextDouble() * 500;  // 300-800
                        break;
                    case "MacBook":
                        price = 1200 + random.nextDouble() * 800; // 1200-2000
                        break;
                    case "AirPods":
                        price = 150 + random.nextDouble() * 100;  // 150-250
                        break;
                    case "Apple Watch":
                        price = 250 + random.nextDouble() * 300;  // 250-550
                        break;
                    case "iMac":
                        price = 1500 + random.nextDouble() * 1000; // 1500-2500
                        break;
                    default:
                        price = 100 + random.nextDouble() * 200;
                }
                
                Order order = new Order(
                    orderId,
                    userId,
                    productName,
                    Math.round(price * 100.0) / 100.0,  // 保留两位小数
                    System.currentTimeMillis()
                );
                
                ctx.collect(order);
                
                // 随机间隔1-3秒
                Thread.sleep(1000 + random.nextInt(2000));
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
}

/*
依赖配置：
需要在pom.xml中添加Table API依赖：

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-java-bridge_2.11</artifactId>
    <version>1.9.0</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-blink_2.11</artifactId>
    <version>1.9.0</version>
</dependency>

使用说明：
1. 直接运行程序，会自动生成模拟的订单数据

2. 观察控制台输出，会看到多种查询结果：
   - 高价值订单（价格>100）
   - 用户订单统计
   - 10秒时间窗口统计
   - 热销产品分析
   - 产品销售分析

3. 输出示例：
   高价值订单> +I[order_000001, user_001, iPhone, 899.50]
   用户订单统计> +I[user_001, 3, 2150.75, 716.92]
   10秒窗口统计> +I[2021-12-31 10:00:00.0, 2021-12-31 10:00:10.0, 5, 3250.80]
   热销产品> +I[iPhone, 1799.00, 2]
   产品销售分析> +I[MacBook, 3200.50, 1650.25, 2]

学习要点：
- StreamTableEnvironment：Table环境
- fromDataStream：流转表
- toAppendStream：表转追加流
- toRetractStream：表转撤回流
- Table API链式操作
- SQL查询语法
- 时间属性和窗口函数
- 聚合查询和分组

Table API vs DataStream API：
1. Table API：声明式，类似SQL，更易理解
2. DataStream API：命令式，更灵活，性能更好
3. 可以相互转换，结合使用

SQL支持的功能：
1. 基本查询：SELECT, WHERE, GROUP BY, HAVING
2. 聚合函数：COUNT, SUM, AVG, MIN, MAX
3. 窗口函数：TUMBLE, HOP, SESSION
4. 时间函数：TUMBLE_START, TUMBLE_END
5. 连接查询：INNER JOIN, LEFT JOIN, RIGHT JOIN
6. 子查询和CTE（公共表表达式）

流表二元性：
1. 追加流：只有插入操作
2. 撤回流：有插入、更新、删除操作
3. 更新插入流：有插入和更新操作

最佳实践：
1. 合理选择时间属性（处理时间vs事件时间）
2. 优化查询性能，避免状态过大
3. 使用合适的流表转换模式
4. 考虑数据的有界性和无界性
5. 合理设计表结构和索引
*/