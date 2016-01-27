package chapter2

/**
 * Created by Fansy on 2016/1/26.
 */

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import utils.Utils

object TestInitialSparkContext extends App{
  val inputFile = "hdfs://node88:8020/user/Administrator/naivebayes.txt"
  val outputFile = "hdfs://node88:8020/user/Administrator/wc_00";
  val path:Path = new Path(outputFile)//得到一个Path
  val hConf:Configuration = Utils.getHadoopConf

  val fs :FileSystem = Utils.getFs
  if(fs.exists(path)){//判断是否存在
    fs.delete(path,true)
    println("存在此文件，已经删除!")
  }else{
    println("文件不存在,开始生成.....!")
  }
  val conf = new SparkConf().setMaster("spark://node88:7077").setAppName("My App")
  val sc = new SparkContext(conf)
  sc.addJar("E:\\fansy_work\\project\\Spark141\\out\\artifacts\\Spark141_jar\\Spark141.jar");

  // Load our input data.
  val input = sc.textFile(inputFile)
  // Split it up into words.
  val words = input.flatMap(line => line.split(" "))
  // Transform into pairs and count.
  val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
  // Save the word count back out to a text file, causing evaluation.
  counts.saveAsTextFile(outputFile)

  sc.stop()
}
