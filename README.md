# TiKV Java CDC Client

独立的 TiKV/TiDB CDC Java SDK。它负责订阅 TiKV CDC gRPC 流、处理 Region 变化并输出
按 resolved timestamp 推进的变更事件，不依赖 Flink。

## 构建

```bash
mvn clean install
```

SDK 当前与 `org.tikv:tikv-client-java:3.3.4-SNAPSHOT` 配套使用，因此需要先确保该构件
存在于 Maven 仓库中。

## Maven 依赖

```xml
<dependency>
    <groupId>org.tikv</groupId>
    <artifactId>java-client-cdc</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

## 读取变更事件

```java
TiConfiguration configuration = TiConfiguration.createDefault("127.0.0.1:2379");
CDCConfig cdcConfig = new CDCConfig();

// Long.MAX_VALUE 表示持续读取；也可传入 TiDB commit ts 作为停止位点。
CDCClient client =
        new CDCClient(configuration, "inventory", "products", Long.MAX_VALUE, cdcConfig);
client.addListener(event -> System.out.println(event));
client.start(startCommitTs);

// 应用退出时释放连接和线程。
client.close();
```

主要 API 位于 `org.tikv.cdc`、`org.tikv.cdc.kv` 和 `org.tikv.cdc.model` 包。
