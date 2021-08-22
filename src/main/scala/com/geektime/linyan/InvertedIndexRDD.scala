package com.geektime.linyan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object InvertedIndexRDD {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().master("local[*]").appName("InvertedIndexRDD").getOrCreate()
//    val sc = spark.sparkContext
    val conf = new SparkConf()
    conf.setAppName("InvertedIndexRDD") // 设置Spark应用名
    conf.setMaster("local[*]") // 设置本地模式
    val sc = new SparkContext(conf)
    // 读取本地数据，生成一个RDD[(String, String)
    val files = sc.wholeTextFiles("file:///Users/winchester/IdeaProjects/spark_api_hw/src/main/resources/input")
    val wordIndexResult = files.flatMap{x =>
        // 通过/分段然后获取最后的文件名并保留.前的部分
        val fileName = x._1.split(s"/").last.split("\\.").head
        // 先按行用换行符分，再按列用空格分词
        x._2.split("\r\n").flatMap(_.split(" ").map{y => (y, fileName)})}
    // 计算每个词在每个文档中出现次数
    val result = wordIndexResult.map(x => ((x._1, x._2), 1)).reduceByKey(_ + _)
      // 按照单词分组和排序，把文档和出现次数组合并写入字符串数组并排序
      .map(x => (x._1._1, (x._1._2,x._2))).groupByKey().sortByKey()
      .map{x =>
          val freqInDoc = x._2.iterator
          val docList = ArrayBuffer[String]()
          while (freqInDoc.hasNext) {
              val docStr = freqInDoc.next().toString()
              docList += docStr
          }
          Tuple2(x._1, docList.sorted)
      }
    // 中间结果调试
//    result.foreach(println)
    // 结果打印
    result.map(x => {
      var docStr = ""
      docStr += "\"" + x._1 + "\": "
      var firstDoc = true
      for (y <- x._2) {
        if (firstDoc) {
          docStr += "{" + y
          firstDoc = false
        } else {
          docStr += "," + y
        }
      }
      docStr += "}"
      docStr
    }).foreach(println)

    sc.stop()
    println( "Hello World!" )
  }
}
