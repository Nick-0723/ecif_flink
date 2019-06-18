package com.nick.flink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
/**
  * Created by nick on 19.6.14
  */

class HBaseSink(tableName: String, family: String) extends RichSinkFunction[String] {


  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, "test-hadoop3:2181,test-hadoop4:2181,test-hadoop5:2181")
    conn = ConnectionFactory.createConnection(conf)
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
//    廖淑霞|N|20181116|2018-10-09|廖淑霞|452701107010245027|11700586|156350800|0|364400|0|a|N|0|2014-12-11|||0||a|1|||||||||N|
    val fields = value.split(s"|",-1)

    val t: Table = conn.getTable(TableName.valueOf(tableName))

    val put: Put = new Put(Bytes.toBytes(fields(6)))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"), Bytes.toBytes(fields(4)))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("id_no"), Bytes.toBytes(fields(5)))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("id"), Bytes.toBytes(fields(6)))
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes("city"), Bytes.toBytes(fields(7)))
    t.put(put)
    t.close()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }
}
