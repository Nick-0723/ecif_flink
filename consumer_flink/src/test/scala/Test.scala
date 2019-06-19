/**
  * Created by nick on 19.6.19
  */
object Test {

  def main(args: Array[String]): Unit = {
    fun()
  }

  def fun(): Unit = {
    val string = "廖淑霞|N|20181116|2018-10-09|廖淑霞|452701107010245027|11700586|156350800|0|364400|0|a|N|0|2014-12-11|||0||a|1|||||||||N|"
    println(string.split("[|]", -1).toList)
  }

}
