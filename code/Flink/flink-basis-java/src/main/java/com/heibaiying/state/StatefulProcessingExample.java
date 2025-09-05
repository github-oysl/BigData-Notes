package com.heibaiying.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Flink 状态管理示例
 * 
 * 功能说明：
 * 1. 演示ValueState的使用 - 存储单个值
 * 2. 演示ListState的使用 - 存储列表数据
 * 3. 展示状态的持久化和恢复
 * 4. 实现基于状态的业务逻辑
 * 
 * 适用场景：
 * - 用户行为分析（记录用户历史行为）
 * - 异常检测（基于历史数据判断异常）
 * - 会话分析（维护用户会话状态）
 * - 累计统计（如累计订单金额、访问次数等）
 * - 去重处理（基于状态记录已处理的数据）
 * 
 * @author heibaiying
 */
public class StatefulProcessingExample {

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
        
        // 启用检查点，用于状态的持久化和故障恢复
        env.enableCheckpointing(5000); // 每5秒创建一次检查点
        
        // 2. 创建模拟数据源 - 生成用户行为数据
        DataStream<UserBehavior> userBehaviorStream = env.addSource(new UserBehaviorSource());
        
        // 3. 使用状态进行用户行为分析
        DataStream<String> analysisResult = userBehaviorStream
            .keyBy(behavior -> behavior.userId)  // 按用户ID分组
            .flatMap(new UserBehaviorAnalyzer());  // 有状态的分析函数
        
        // 4. 输出分析结果
        analysisResult.print();
        
        // 5. 启动执行
        env.execute("Stateful Processing Example");
    }
    
    /**
     * 用户行为数据类
     */
    public static class UserBehavior {
        public String userId;        // 用户ID
        public String action;        // 行为类型（view, click, purchase等）
        public double amount;        // 金额（购买行为时有值）
        public long timestamp;       // 时间戳
        
        public UserBehavior() {}
        
        public UserBehavior(String userId, String action, double amount, long timestamp) {
            this.userId = userId;
            this.action = action;
            this.amount = amount;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    ", amount=" + amount +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
    
    /**
     * 模拟用户行为数据源
     */
    public static class UserBehaviorSource implements SourceFunction<UserBehavior> {
        private boolean running = true;
        private Random random = new Random();
        private String[] userIds = {"user_001", "user_002", "user_003", "user_004"};
        private String[] actions = {"view", "click", "add_to_cart", "purchase"};
        
        @Override
        public void run(SourceContext<UserBehavior> ctx) throws Exception {
            while (running) {
                // 随机生成用户行为数据
                String userId = userIds[random.nextInt(userIds.length)];
                String action = actions[random.nextInt(actions.length)];
                double amount = action.equals("purchase") ? 10 + random.nextDouble() * 100 : 0;
                
                UserBehavior behavior = new UserBehavior(
                    userId, 
                    action, 
                    Math.round(amount * 100.0) / 100.0,
                    System.currentTimeMillis()
                );
                
                ctx.collect(behavior);
                
                // 每1-3秒生成一条数据
                Thread.sleep(1000 + random.nextInt(2000));
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
    
    /**
     * 用户行为分析器
     * 使用状态来维护用户的历史行为信息
     */
    public static class UserBehaviorAnalyzer extends RichFlatMapFunction<UserBehavior, String> {
        
        // ValueState：存储用户的累计购买金额
        private transient ValueState<Double> totalAmountState;
        
        // ValueState：存储用户的行为计数
        private transient ValueState<Integer> behaviorCountState;
        
        // ListState：存储用户最近的行为历史（最多保留10条）
        private transient ListState<String> recentActionsState;
        
        /**
         * 初始化状态
         * 在函数实例化时调用，用于创建状态描述符
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建ValueState用于存储累计购买金额
            ValueStateDescriptor<Double> totalAmountDescriptor = new ValueStateDescriptor<>(
                "totalAmount",  // 状态名称
                Double.class,   // 状态类型
                0.0            // 默认值
            );
            totalAmountState = getRuntimeContext().getState(totalAmountDescriptor);
            
            // 创建ValueState用于存储行为计数
            ValueStateDescriptor<Integer> behaviorCountDescriptor = new ValueStateDescriptor<>(
                "behaviorCount",
                Integer.class,
                0
            );
            behaviorCountState = getRuntimeContext().getState(behaviorCountDescriptor);
            
            // 创建ListState用于存储最近的行为历史
            ListStateDescriptor<String> recentActionsDescriptor = new ListStateDescriptor<>(
                "recentActions",
                TypeInformation.of(new TypeHint<String>() {})
            );
            recentActionsState = getRuntimeContext().getListState(recentActionsDescriptor);
        }
        
        /**
         * 处理每个用户行为事件
         */
        @Override
        public void flatMap(UserBehavior behavior, Collector<String> out) throws Exception {
            
            // 1. 更新行为计数
            Integer currentCount = behaviorCountState.value();
            behaviorCountState.update(currentCount + 1);
            
            // 2. 如果是购买行为，更新累计金额
            if ("purchase".equals(behavior.action)) {
                Double currentAmount = totalAmountState.value();
                totalAmountState.update(currentAmount + behavior.amount);
            }
            
            // 3. 更新最近行为历史
            List<String> recentActions = new ArrayList<>();
            for (String action : recentActionsState.get()) {
                recentActions.add(action);
            }
            
            // 添加当前行为
            recentActions.add(behavior.action);
            
            // 只保留最近10条行为记录
            if (recentActions.size() > 10) {
                recentActions = recentActions.subList(recentActions.size() - 10, recentActions.size());
            }
            
            // 更新状态
            recentActionsState.clear();
            recentActionsState.addAll(recentActions);
            
            // 4. 生成分析结果
            StringBuilder result = new StringBuilder();
            result.append("用户 ").append(behavior.userId).append(" 分析结果: ");
            result.append("总行为次数=").append(behaviorCountState.value());
            result.append(", 累计购买金额=").append(String.format("%.2f", totalAmountState.value()));
            result.append(", 最近行为=").append(recentActions);
            
            // 5. 异常检测：如果用户在短时间内有异常行为模式，发出警告
            if (detectAnomalousPattern(recentActions)) {
                result.append(" [警告: 检测到异常行为模式!]");
            }
            
            // 6. VIP用户识别：累计购买金额超过500的用户
            if (totalAmountState.value() > 500) {
                result.append(" [VIP用户]");
            }
            
            out.collect(result.toString());
        }
        
        /**
         * 检测异常行为模式
         * 简单的规则：连续3次相同行为可能是异常
         */
        private boolean detectAnomalousPattern(List<String> recentActions) {
            if (recentActions.size() < 3) {
                return false;
            }
            
            // 检查最近3次行为是否相同
            int size = recentActions.size();
            String lastAction = recentActions.get(size - 1);
            String secondLastAction = recentActions.get(size - 2);
            String thirdLastAction = recentActions.get(size - 3);
            
            return lastAction.equals(secondLastAction) && secondLastAction.equals(thirdLastAction);
        }
    }
}

/*
使用说明：
1. 直接运行程序，会自动生成模拟的用户行为数据

2. 观察控制台输出，会看到每个用户的实时分析结果：
   - 总行为次数统计
   - 累计购买金额
   - 最近行为历史
   - 异常行为检测
   - VIP用户识别

3. 输出示例：
   用户 user_001 分析结果: 总行为次数=5, 累计购买金额=156.78, 最近行为=[view, click, purchase, view, click] [VIP用户]
   用户 user_002 分析结果: 总行为次数=3, 累计购买金额=0.00, 最近行为=[view, view, view] [警告: 检测到异常行为模式!]

学习要点：
- ValueState：存储单个值的状态
- ListState：存储列表数据的状态
- 状态描述符（StateDescriptor）
- 状态的生命周期管理
- 检查点（Checkpoint）机制
- 状态的序列化和反序列化
- 基于状态的业务逻辑实现

状态管理最佳实践：
1. 合理设置状态TTL，避免状态无限增长
2. 选择合适的状态后端（MemoryStateBackend、FsStateBackend、RocksDBStateBackend）
3. 定期清理不需要的状态
4. 考虑状态的可扩展性和性能影响
5. 使用状态处理器API进行状态迁移和修复
*/