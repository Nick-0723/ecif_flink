package com.nick.flink

import java.util.Properties

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSource

/**
  * Created by nick on 19.6.5
  */
object KafkaConsumerFlink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "test-hadoop5:9092")
    properties.setProperty("zookeeper.connect", "test-hadoop3:2181,test-hadoop4:2181,test-hadoop5:2181")
    properties.setProperty("group.id", "ecif")

    val myConsumer = new FlinkKafkaConsumer011[String]("ecif", new SimpleStringSchema, properties)
    val dstream: DataStreamSource[String] = env.addSource(myConsumer)

    dstream.map(s => {
      val fileds = s.split(s"|",-1)


    })

    val hbaseSink = dstream.addSink(new HBaseSink("cust_info","f")).name("hbaseSink")

    env.execute("nick_test_flink")
  }
}
