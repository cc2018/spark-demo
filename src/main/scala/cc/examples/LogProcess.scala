package cc.examples

import org.apache.spark.sql.{Row, SparkSession}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.NameFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._

object LogProcess {
  // 再使用name filter 定制序列化列名称
  val nameFilter = new NameFilter() {
    override def process(o: Any, propertyName: String, propertyValue: Any): String = {
      // 所有属性全部小写定义
      // propertyName.toLowerCase()

      var name:String = propertyName

      if (propertyName.equals("_url")) {
        name = "x_url"
      } else if (propertyName.equals("bd_uuid")) {
        name = "BAIDUID"
      } else if (propertyName.equals("user_created_time")) {
        name = "user_created_time"
      } else if (propertyName.equals("modid")) {
        name = "modId"
      }

      name
    }
  }

  def parseLine(line: String): Row = {
    var result:Row = null

    try {
      // 按schema顺序构建row
      val lineObj = JSON.parseObject(line)

      // 使用json对象处理value
      val baiduId = lineObj.get("BAIDUID")
      val guid = lineObj.get("guid")

      result =  Row(
        lineObj.get("BAIDUID"),
        lineObj.get("ac"),
        lineObj.get("country"),
        lineObj.get("fr"),
        lineObj.get("host"),
        lineObj.get("ip_code"),
        lineObj.get("level"),
        lineObj.get("linkIndex"),
        lineObj.get("modId"),
        lineObj.get("page"),
        lineObj.get("position"),
        lineObj.get("r"),
        lineObj.get("tn"),
        lineObj.get("type"),
        lineObj.get("url"),
        lineObj.get("globalhao123_bdtime"),
        lineObj.get("x_url"),
        lineObj.get("guid")
      )
    } catch {
      case e: Exception => println(e)
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Log process")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("file:/Users/caojian02/cc-pro/spark-log/org-logs/hao123/20171203/*")
    val rdd2 = rdd.map(parseLine).filter(line => line != null)

    // rdd2.collect().foreach(println)
    // println(rdd2.count())

    //设置schema结构
    val schema = StructType(
      Seq(
        StructField("BAIDUID", StringType, true)
        , StructField("ac", StringType, true)
        , StructField("country", StringType, true)
        , StructField("fr", StringType, true)
        , StructField("host", StringType, true)
        , StructField("ip_code", StringType, true)
        , StructField("level", StringType, true)
        , StructField("linkIndex", StringType, true)
        , StructField("modId", StringType, true)
        , StructField("page", StringType, true)
        , StructField("position", StringType, true)
        , StructField("r", StringType, true)
        , StructField("tn", StringType, true)
        , StructField("type", StringType, true)
        , StructField("url", StringType, true)
        , StructField("globalhao123_bdtime", StringType, true)
        , StructField("x_url", StringType, true)
        , StructField("guid", StringType, true)
      )
    )

    val df = spark.createDataFrame(rdd2, schema)
    df.createOrReplaceTempView("TEST_TABLE")
    spark.sql("SELECT BAIDUID, country, page FROM TEST_TABLE").show()
    println(df.count())

    spark.stop()
  }

}
