package com.geektime.linyan

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer

object SparkDistCp2 {

  var fileList = ArrayBuffer[(Path, Path)]()

  def main(args: Array[String]): Unit = {

    val ignoreFailure = "FALSE"
    val maxConcurrence = 1
    val sourcePath = "hdfs://localhost:9000/linyan"
    val targetPath = "/output_test4"
//    val sourcePath = "file:/Users/winchester/IdeaProjects/spark_api_hw/src/main/resources/input"
//    val targetPath = "/Users/winchester/IdeaProjects/spark_api_hw/src/main/resources/output_test2"

    lazy val session = SparkSession.builder().appName("SparkDistCp").master("local[*]").getOrCreate()
    val sc = session.sparkContext
//    sc.setLogLevel("WARN")
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fsURI = fs.getUri

    val fileListCopy = checkDirectory(new Path(sourcePath), fs, new Path(sourcePath), new Path(targetPath), sc)
    fileListCopy.foreach(println)
    val fileStrList = fileList.map((x) => (x._1.toString, fsURI + x._2.toString))
    val rdd = sc.makeRDD(fileStrList, maxConcurrence)
    rdd.mapPartitions(value => {
      val res = ArrayBuffer[String]()

//      val rddFS = FileSystem.get(rddSC.hadoopConfiguration)
//      val rddConf = new Configuration()
      while (value.hasNext) {
        val item = value.next()
        val rddSC = session.sparkContext
        val rddFS = new Path(item._1).getFileSystem(new Configuration(rddSC.hadoopConfiguration))
        try {
          val flag = FileUtil.copy(rddFS, new Path(item._1), rddFS, new Path(item._2), false, new Configuration(rddSC.hadoopConfiguration))
          if (flag) {
            res.append(item._1 + " copied to " + item._2 + " successfully.")
          }
        } catch {
          case ex: Exception => {
            if ("TRUE".equals(ignoreFailure)) {
              println(item._1 + " Copy Failed.")
            } else {
              System.exit(1)
            }
          }
        }
      }
      res.iterator
    }).foreach(println)
//    sc.stop()
  }

    def checkDirectory(sourcePath: Path, fs: FileSystem, sourceFullPath: Path, targetPath: Path, sc: SparkContext): ArrayBuffer[(Path, Path)] = {
      // hdfs.listStatus(path) is an array of FileStatus objects for the files under the given path
      val fs = sourcePath.getFileSystem(sc.hadoopConfiguration)
      fs.listStatus(sourceFullPath).foreach { status => {
        println(status.getPath.toString)
        val subPath = status.getPath.toString.split(sourcePath.toString)(1)
        println(subPath)
        val pathStr = targetPath.toString + subPath
        println(pathStr)
        if (status.isDirectory) {
          val dirCreated = fs.mkdirs(new Path(pathStr))
          println(dirCreated)
          val tPathSub = new Path(pathStr)
          checkDirectory(sourcePath, fs, status.getPath, tPathSub, sc)
        }
        else {
          fileList.append((status.getPath, new Path(pathStr)))
        }
      }
      }
      fileList
    }
  }

