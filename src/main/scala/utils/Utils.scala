package utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.hadoop.fs.Path
/**
 * Created by Fansy on 2016/1/26.
 */
class Utils {

}
object Utils{
  val HNODE="node1" // hadoop
  val SNODE="node2" // spark
  def getHadoopConf ={
    val conf:Configuration = new Configuration()
    conf.set("","")
    conf.set("mapred.remote.os","Linux");
    conf.set("fs.defaultFS", HNODE+":8020");// 指定namenode
    conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
    conf.set("yarn.resourcemanager.address",HNODE+":8032"); // 指定resourcemanager
//    conf.set("yarn.resourcemanager.scheduler.address", SCHEDULER);// 指定资源分配器
//    conf.set("mapreduce.jobhistory.address", JOBHISTORY);
    conf
  }
  def getFs={
    FileSystem.get(getHadoopConf)
  }
  def delHDFS(file:String,delSub:Boolean)={
    val path = new Path(file)
    if(getFs.exists(path)) getFs.delete(path,delSub)
  }

  def getSparkConf():SparkContext={
     getSparkConf("default")
  }

  def getSparkConf( name:String)={
    val conf = new SparkConf().setMaster(Utils.MASTER).setAppName(name)

    val sc = new SparkContext(conf)
    //  sc.addJar("E:\\fansy_work\\project\\Spark141\\out\\artifacts\\Spark141_jar\\Spark141.jar");
    sc.addJar("C:\\Users\\fansy\\workspace\\learn_spark\\out\\artifacts\\Spark141_jar\\Spark141.jar")
    sc
  }

  val MASTER="spark://"+SNODE+":7077"
  val HDFS="hdfs://"+HNODE+":8020"
  private var root_url=new File("").getAbsolutePath
  def ROOTURL()= {
    if(root_url==null){
      root_url= new File("").getAbsolutePath
    }
    root_url
  }
}
