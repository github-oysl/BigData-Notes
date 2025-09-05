package com.heibaiying.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Flink CEP (Complex Event Processing) 复杂事件处理示例
 * 
 * 功能说明：
 * 1. 演示复杂事件模式匹配
 * 2. 实现异常行为检测
 * 3. 展示时间窗口内的事件序列分析
 * 4. 实现业务规则引擎
 * 
 * 适用场景：
 * - 欺诈检测（异常交易模式识别）
 * - 系统监控（故障模式检测）
 * - 用户行为分析（异常行为识别）
 * - 物联网设备监控（设备故障预警）
 * - 金融风控（风险事件识别）
 * 
 * 注意：此示例需要CEP依赖，需要在pom.xml中添加相应依赖
 * 
 * @author heibaiying
 */
public class ComplexEventProcessingExample {

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
        
        // 2. 创建模拟登录事件数据源
        DataStream<LoginEvent> loginEventStream = env.addSource(new LoginEventSource());
        
        // 3. 按用户ID分组
        DataStream<LoginEvent> keyedStream = loginEventStream.keyBy(event -> event.userId);
        
        // 4. 定义复杂事件模式 - 检测连续登录失败
        Pattern<LoginEvent, ?> loginFailPattern = Pattern.<LoginEvent>begin("first")
            // 第一次登录失败
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return "FAIL".equals(event.status);
                }
            })
            // 紧接着第二次登录失败
            .next("second")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return "FAIL".equals(event.status);
                }
            })
            // 紧接着第三次登录失败
            .next("third")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return "FAIL".equals(event.status);
                }
            })
            // 在10秒内发生
            .within(Time.seconds(10));
        
        // 5. 应用模式到数据流
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, loginFailPattern);
        
        // 6. 处理匹配的模式
        DataStream<String> alertStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                LoginEvent first = pattern.get("first").get(0);
                LoginEvent second = pattern.get("second").get(0);
                LoginEvent third = pattern.get("third").get(0);
                
                return String.format(
                    "[安全警报] 用户 %s 在10秒内连续3次登录失败！" +
                    "失败时间: %d, %d, %d. 可能存在暴力破解攻击！",
                    first.userId,
                    first.timestamp,
                    second.timestamp,
                    third.timestamp
                );
            }
        });
        
        // 7. 定义另一个模式 - 检测异常登录地点
        Pattern<LoginEvent, ?> locationPattern = Pattern.<LoginEvent>begin("normal")
            // 正常登录
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return "SUCCESS".equals(event.status);
                }
            })
            // 紧接着从不同地点登录
            .next("suspicious")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent event) {
                    return "SUCCESS".equals(event.status);
                }
            })
            // 在5分钟内发生
            .within(Time.minutes(5));
        
        PatternStream<LoginEvent> locationPatternStream = CEP.pattern(keyedStream, locationPattern);
        
        DataStream<String> locationAlertStream = locationPatternStream.select(
            new PatternSelectFunction<LoginEvent, String>() {
                @Override
                public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                    LoginEvent normal = pattern.get("normal").get(0);
                    LoginEvent suspicious = pattern.get("suspicious").get(0);
                    
                    // 检查是否来自不同地点（简单的地点检查）
                    if (!normal.location.equals(suspicious.location)) {
                        return String.format(
                            "[地点异常] 用户 %s 在5分钟内从不同地点登录！" +
                            "地点1: %s (时间: %d), 地点2: %s (时间: %d)",
                            normal.userId,
                            normal.location,
                            normal.timestamp,
                            suspicious.location,
                            suspicious.timestamp
                        );
                    }
                    return null;
                }
            }
        ).filter(alert -> alert != null);
        
        // 8. 输出警报信息
        alertStream.print("登录失败警报");
        locationAlertStream.print("地点异常警报");
        
        // 9. 同时输出原始事件用于调试
        loginEventStream.print("登录事件");
        
        // 10. 启动执行
        env.execute("Complex Event Processing Example");
    }
    
    /**
     * 登录事件数据类
     */
    public static class LoginEvent {
        public String userId;        // 用户ID
        public String status;        // 登录状态：SUCCESS, FAIL
        public String location;      // 登录地点
        public long timestamp;       // 时间戳
        
        public LoginEvent() {}
        
        public LoginEvent(String userId, String status, String location, long timestamp) {
            this.userId = userId;
            this.status = status;
            this.location = location;
            this.timestamp = timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("LoginEvent{userId='%s', status='%s', location='%s', timestamp=%d}",
                userId, status, location, timestamp);
        }
    }
    
    /**
     * 模拟登录事件数据源
     */
    public static class LoginEventSource implements SourceFunction<LoginEvent> {
        private boolean running = true;
        private Random random = new Random();
        private String[] userIds = {"user_001", "user_002", "user_003", "user_004"};
        private String[] locations = {"北京", "上海", "广州", "深圳", "杭州"};
        private String[] statuses = {"SUCCESS", "FAIL"};
        
        @Override
        public void run(SourceContext<LoginEvent> ctx) throws Exception {
            while (running) {
                String userId = userIds[random.nextInt(userIds.length)];
                String location = locations[random.nextInt(locations.length)];
                
                // 模拟不同的登录场景
                String status;
                if (random.nextDouble() < 0.7) {
                    // 70% 成功登录
                    status = "SUCCESS";
                } else {
                    // 30% 失败登录
                    status = "FAIL";
                    
                    // 模拟连续失败的情况
                    if ("user_002".equals(userId) && random.nextDouble() < 0.8) {
                        // user_002 有80%概率连续失败（用于触发警报）
                        for (int i = 0; i < 3; i++) {
                            LoginEvent failEvent = new LoginEvent(
                                userId, 
                                "FAIL", 
                                location, 
                                System.currentTimeMillis()
                            );
                            ctx.collect(failEvent);
                            Thread.sleep(1000); // 间隔1秒
                        }
                        Thread.sleep(5000); // 等待5秒再继续
                        continue;
                    }
                }
                
                LoginEvent event = new LoginEvent(
                    userId, 
                    status, 
                    location, 
                    System.currentTimeMillis()
                );
                
                ctx.collect(event);
                
                // 模拟异地登录场景
                if ("SUCCESS".equals(status) && "user_003".equals(userId) && random.nextDouble() < 0.3) {
                    // user_003 有30%概率异地登录
                    Thread.sleep(2000);
                    String differentLocation = locations[random.nextInt(locations.length)];
                    while (differentLocation.equals(location)) {
                        differentLocation = locations[random.nextInt(locations.length)];
                    }
                    
                    LoginEvent locationEvent = new LoginEvent(
                        userId, 
                        "SUCCESS", 
                        differentLocation, 
                        System.currentTimeMillis()
                    );
                    ctx.collect(locationEvent);
                }
                
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
需要在pom.xml中添加CEP依赖：

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep_2.11</artifactId>
    <version>1.9.0</version>
</dependency>

使用说明：
1. 直接运行程序，会自动生成模拟的登录事件数据

2. 观察控制台输出，会看到：
   - 原始登录事件
   - 连续登录失败警报
   - 异地登录警报

3. 输出示例：
   登录事件> LoginEvent{userId='user_002', status='FAIL', location='北京', timestamp=1640995200000}
   登录事件> LoginEvent{userId='user_002', status='FAIL', location='北京', timestamp=1640995201000}
   登录事件> LoginEvent{userId='user_002', status='FAIL', location='北京', timestamp=1640995202000}
   登录失败警报> [安全警报] 用户 user_002 在10秒内连续3次登录失败！失败时间: 1640995200000, 1640995201000, 1640995202000. 可能存在暴力破解攻击！
   
   地点异常警报> [地点异常] 用户 user_003 在5分钟内从不同地点登录！地点1: 上海 (时间: 1640995210000), 地点2: 深圳 (时间: 1640995212000)

学习要点：
- Pattern API：定义复杂事件模式
- SimpleCondition：简单条件过滤
- next()：严格连续模式
- followedBy()：松散连续模式
- within()：时间窗口约束
- PatternSelectFunction：模式匹配结果处理
- CEP.pattern()：应用模式到数据流

CEP模式类型：
1. 严格连续（next）：事件必须严格按顺序出现
2. 松散连续（followedBy）：事件按顺序出现，但中间可以有其他事件
3. 非确定性松散连续（followedByAny）：匹配所有可能的序列

高级特性：
1. 量词模式：oneOrMore(), times(), optional()
2. 条件组合：where().or().where()
3. 迭代条件：IterativeCondition
4. 模式组：Pattern.begin().subtype()
5. 超时处理：PatternTimeoutFunction

实际应用建议：
1. 合理设计模式复杂度，避免性能问题
2. 设置合适的时间窗口
3. 考虑状态大小和内存使用
4. 处理模式匹配的延迟
5. 结合业务规则设计模式
*/