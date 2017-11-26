package cc.examples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
    var file = ""
    if (args.length < 1) {
      System.err.println("No input file, use default hdfs file for test")
      file = "hdfs://localhost:9100/test/hellospark"
    } else {
      file = args(0)
    }

    // hdfs://localhost:9100/test/hellospark
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val line = sc.textFile(file)

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
    sc.stop()
  }
}
