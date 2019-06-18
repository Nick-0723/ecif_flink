import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat

/**
  * Created by nick on 19.6.10
  */
object Test {
  def main(args:Array[String]):Unit = {
    val conf = new Configuration()
    conf.set("hive.metastore.local", "false")
    conf.set("hive.metastore.uris", "thrift://ip:9083")

    val env = ExecutionEnvironment.getExecutionEnvironment
//    val dataset = env.createInput(HCatInputFormat.setInput(conf,"ecifdb","pty_cert",null))
//
//    dataset.first(10).print()
//
//    env.execute("flink hive test")
  }
}
