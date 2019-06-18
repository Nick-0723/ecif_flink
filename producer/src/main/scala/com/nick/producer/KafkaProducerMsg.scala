package com.nick.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.StdIn



/**
  * Created by nick on 19.6.5
  */
class KafkaProducerMsg(brokerList : String, topic : String) extends Runnable{

  private val BROKER_LIST = brokerList // test-hadoop5:9092,test-hadoop6:9092,test-hadoop7:9092,test-hadoop8:9092,test-hadoop9:9092,
  private val TARGET_TOPIC = topic

  /**
    * 1、配置属性
    * metadata.broker.list : kafka集群的broker，只需指定2个即可
    * serializer.class : 如何序列化发送消息
    * request.required.acks : 1代表需要broker接收到消息后acknowledgment,默认是0
    *   0:producer不会等待broker发送ack
    * 　1:当leader接收到消息之后发送ack
    * 　-1:当所有的follower都同步消息成功后发送ack
    * producer.type : 默认就是同步sync
    */
  private val props = new Properties()
  props.put("metadata.broker.list", this.BROKER_LIST)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  props.put("producer.type", "async")

  /**
    * 2、创建Producer
    */
  private val producer = new KafkaProducer[String, String](props)

  /**
  * 3、产生并发送消息
  * 搜索目录dir下的所有包含“transaction”的文件并将每行的记录以消息的形式发送到kafka
  *
  */
  def run() : Unit = {
    var i = 0
    while(true){
      try
        //        // 直接发送，如果 broker 没收到就会丢失记录。不知道消息是否发送成功，这种一般适用于允许丢失消息的情况。
        //        producer.send(new ProducerRecord(TARGET_TOPIC, "key-" + i, "value-" + i))
        //
        //        // 同步发送，这里，使用了Future.get()方法，会等待kafka的确认回复。当broker遇到错误或者应用出现问题时，future接口都会抛出异常，然后我们可以捕获到这个异常进行处理。如果没有错误。将会获得RecordMetadata对象，这个对象包含了消息写入的偏移值。
        //        producer.send(new ProducerRecord(TARGET_TOPIC, "key-" + i, "value-" + i)).get()

        // 异步发送，
        // 从控制台输入， key 和 value 用空格分隔，value 自己用 "," 分隔。
        {val str = StdIn.readLine().split(" ")
        producer.send(new ProducerRecord(TARGET_TOPIC, str(0), str(1)), callback)

        i += 1}
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }


  /**
    * 回调函数 如果有异常抛出，则 metadata 为 null，e 不为 null；如果没有异常，metadata 不为 null，e 为 null。
    * @param metadata 源数据信息
    * @param e 错误信息
    */
  def callback(metadata: RecordMetadata, e: Exception): Unit = {
    if (e != null){
      e.printStackTrace()
    } else {
      println("The offset of the record we just sent is: " + metadata.offset())
    }
  }

}

object KafkaProducerMsg extends App {
  new KafkaProducerMsg("test-hadoop5:9092,test-hadoop6:9092,test-hadoop7:9092,test-hadoop8:9092,test-hadoop9:9092","ecif").run()
}

