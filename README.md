# ecif_flink
kafka, java, scala, flink, hbase

测试通过 Flink 消费 Kafka 端数据，处理之后往 HBase 表里面存。

## 版本

Flink 集群版本 1.7.2

## 注意的点

- Flink 的 lib 包目录下除了自带的 jar 包不要添加其它的。
- 因为 Flink 官方希望你将所有的依赖和业务逻辑打成一个 fat jar，这样方便提交，因为 Flink 认为你应该对自己的业务逻辑做好单元测试，而不应该把这部分测试工作频繁提交到集群去做。
- 代码里面的 HBase 版本跟集群的 HBase 版本无关。
- 1.4 以下的 HBase jar 包 Flink 已经不支持了。
