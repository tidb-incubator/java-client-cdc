# TiDB CDC SDK 拆分设计

## 目标与边界

将 Flink TiDB CDC connector 内嵌的 `org.tikv` CDC 客户端实现迁移到独立的
`java-client-cdc` Maven 工程。SDK 负责连接 TiKV CDC gRPC、管理 Region、推进 resolved
timestamp、组装 `PolymorphicEvent` 以及提供表 KeyRange/解码工具；Flink connector 只负责
把 Flink split/offset 转成 SDK 参数，并把 SDK 事件交给现有 Debezium/Flink 处理链。

SDK 保留现有 `org.tikv.cdc.*` 包名，避免 connector 发生大范围 API 改写。构件坐标采用
`org.tikv:java-client-cdc:1.0.0-SNAPSHOT`，Java 8，与当前
`org.tikv:tikv-client-java:3.3.4-SNAPSHOT` 对齐。

## 解耦方案

原实现有三类 Flink 反向依赖：`CDCClient` 接收 Flink 的 `CDCEventOffset`，工具类使用 Flink
shaded Guava/Preconditions，`GRPCClient` 判断 Flink shaded Netty executor。SDK 将停止位点改成
纯 Java `long endTs`，`Long.MAX_VALUE` 表示持续读取；connector 从 ending offset 的 commit
version 转换为 `long`。集合和参数校验使用普通 Guava；executor 串行化只识别 SDK/Guava/gRPC
自身类型。由此 SDK 的编译与运行不再需要任何 Flink artifact。

异常通过现有 `EventListener.onException` 传播；客户端关闭、限流、Region 重试和 resolved-ts
推进逻辑保持不变。SDK 携带原有纯单元测试，connector 保留依赖其测试基座的集成测试。

## 验收

1. `java-client-cdc` 可独立执行 Maven test/package。
2. SDK 主源码不存在 `org.apache.flink` import。
3. connector 的主源码目录不再包含 `org/tikv` 实现。
4. connector 通过 `org.tikv:java-client-cdc` 编译，并将 ending offset 正确转换为 endTs。

