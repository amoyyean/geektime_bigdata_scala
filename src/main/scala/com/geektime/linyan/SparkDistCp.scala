package com.geektime.linyan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkDistCp {

  var dirList = ArrayBuffer[String]()
  var fileList = ArrayBuffer[(Path, Path)]()
  var ignoreFailure = "FALSE"
  var maxConcurrence = 1

  val numArg = 2
  val helpText =
    """Spark RDD DistCp
      |Usage: sourcePath targetPath -i Ignore_failures -m max_concurrence
      |sourcePath - source root directory of files
      |targetPath - destination root directory of files, without URI (host, port)
      | -i 是否忽略文件拷贝失败，继续拷贝
      | -m 同时copy的最大并发task数"""

  @transient private val conf = new SparkConf().setAppName("SparkDistCp").setMaster("local[*]")
  @transient private val sc = new SparkContext(conf)
  @transient private val hadoopConf = sc.hadoopConfiguration
  @transient private val fs = FileSystem.get(hadoopConf) //获取Spark关联的Hadoop的FileSystem
  @transient private val fsURI = fs.getUri // 获取Spark关联的Hadoop的FileSystem的URI

  var sourcePath = "hdfs://localhost:9000/input"
  var targetPath = "/output_test4"

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//    conf.setAppName("SparkDistCp") // 设置Spark应用名
//    conf.setMaster("local[*]") // 设置本地模式
//    val sc = new SparkContext(conf)
//    val hadoopConf = sc.hadoopConfiguration
//    val fs = FileSystem.get(hadoopConf) //获取Spark关联的Hadoop的FileSystem
//    val fsURI = fs.getUri // 获取Spark关联的Hadoop的FileSystem的URI

//    val sparkSession = SparkSession.builder().getOrCreate()
//    val config = OptionsParsing.parse(args, sparkSession.sparkContext.hadoopConfiguration)
//    val (src, dest) = config.sourceAndDestPaths
    // 解析参数
//    parseArgs(args)
    checkDirectory(new Path(sourcePath), fs, new Path(targetPath))
//    val fileStrList = fileList.map((s) => (s._1.toString(), s._2.toString))
//    val rdd = sc.makeRDD(fileStrList, maxConcurrence)
//    val r = rdd.mapPartitions(value => {
//      var result = ArrayBuffer[String]()
//      while (value.hasNext) {
//        val index = value.next()
//        try {
//          val flag = FileUtil.copy(fs, new Path(index._1), fs, new Path(index._2), false, hadoopConf)
//          if (flag) {
//            result.append(index._1 + "copied to" + index._2 + "successfully.")
//          }
//        } catch {
//          case ex: Exception => {
//            if ("TRUE".equals(ignoreFailure)) {
//              println(index._1 + " Copy Failed.")
//            } else {
//              System.exit(1)
//            }
//          }
//        }
//      }
//      result.iterator
//    }).foreach(print(_))
    sc.stop()
  }

  def listDirectory(path: Path, hdfs: FileSystem): ArrayBuffer[String] = {
    val filePath = hdfs.listStatus(path)

    filePath.foreach { status => {
      if (status.isDirectory) {
        dirList += status.getPath.toString
        listDirectory(new Path(status.getPath.toString), hdfs)
      }
      else {
        dirList += status.getPath.toString
      }
    }
    }
    dirList.sorted
  }

  def checkDirectory(sourcePath: Path, fs: FileSystem, targetPath: Path): ArrayBuffer[(Path, Path)] = {
    // hdfs.listStatus(path) is an array of FileStatus objects for the files under the given path
    fs.listStatus(sourcePath).foreach { status => {
      //getPath returns absolute full path, make sourcePath and targetPath different in recursion
      val subPath = status.getPath.toString.split(sourcePath.toString)(1)
      println(subPath)
      val pathStr = targetPath.toString + subPath
//      val pathStr = targetPath.toString + subPath
      println(pathStr)
      if (status.isDirectory) {
        val dirCreated = fs.mkdirs(new Path(pathStr))
        println(dirCreated)
        val tPathSub = new Path(pathStr)
        checkDirectory(status.getPath, fs, tPathSub)
      }
      else {
        fileList.append((status.getPath, targetPath))
      }
    }
    }
    fileList
  }

  private def parseArgs(args: Array[String]): Unit = {

    val len = args.length
    if (len < numArg) {
      println(helpText)
      System.exit(1)
    }
    var argsMap: Map[String, String] = Map()
    var i = 0
    while (i < len) {
      if (0 == i) {
        argsMap += ("source_path" -> args(i))
      } else if (1 == i) {
        argsMap += ("target_path" -> args(i))
      } else {
        if ("-i".equals(args(i).toLowerCase)) {
          argsMap += ("ignore_failure" -> "TRUE")
        }
        if ("-m".equals(args(i).toLowerCase)) {
          if (i > len - 1) {
            println("-m 参数值缺失")
            System.exit(1)
          }
          try {
            var mValueValidate = args(i + 1).toInt
          } catch {
            case ex: NumberFormatException => {
              println("-m 参数值含有非法数字")
              System.exit(1)
            }
          }
          argsMap += ("max_concurrence" -> args(i + 1))
          i = i + 1
        }
      }
      i = i + 1
    }
    argsMap.keys.foreach(value => {
      println(value + " : " + argsMap(value))
    })
    if (argsMap.contains("source_path")) {
      sourcePath = argsMap("source_path")
    }
    if (argsMap.contains("target_path")) {
      targetPath = argsMap("target_path")
    }
    if (argsMap.contains("ignore_failures")) {
      ignoreFailure = argsMap("ignore_failure")
    }
    if (argsMap.contains("max_concurrence")) {
      maxConcurrence = argsMap("max_concurrence").toInt
    }
  }

}
