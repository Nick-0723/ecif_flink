package com.nick.producer

import org.apache.flink.api.java
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.types.Row

/**
  * Created by nick on 19.6.10
  */
object HiveHCatalogSourceSQL {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet: java.DataSet[Row] = HiveHCatalogTableSource.builder()
      .database("ecifdb")
      .metastoreUris("thrift://hadoop1:9083")
      .table("ecif_test")
      .field("cust_id", Types.STRING)
      .field("sfz_id", Types.STRING)
      .field("sfz_type", Types.STRING)
      .field("hz_id", Types.STRING)
      .field("hz_type", Types.STRING)
      .build()
      .getDataSet(env.getJavaEnv)

    tableEnv.registerDataSet("ecif_test", new DataSet(dataSet))

    dataSet.print()
    /**
      * cust_id	string
      * 2	sfz_id	string
      * 3	sfz_type	string
      * 4	hz_id	string
      * 5	hz_type	string
      */

  }
}
