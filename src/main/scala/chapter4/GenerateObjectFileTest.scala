package chapter4

import org.apache.spark.rdd.RDD
import utils.Utils

/**
 * ObjectFile测试
 * Created by fansy on 2016/2/2.
 */
object GenerateObjectFileTest extends App{
  val sc = Utils.getSparkConf("pagerank")
  val file = Utils.HDFS+"/user/fansy/pk.dat"
  val outfile = Utils.HDFS+"/user/fansy/pk_obj.dat"

  Utils.delHDFS(outfile,true)
  val data = sc.textFile(file)

  val odata :RDD[(String, Seq[String])]= data.map(line => (getFirst(line),getSecond(line)))

  odata.saveAsObjectFile(outfile)
  sc.stop()

  def getFirst(line:String): String ={
    if(!line.contains('\t')) return line
    line.substring(0,line.indexOf('\t'))
  }
  def getSecond(line:String):Seq[String]={
    if(!line.contains('\t')) return Seq[String]()
    line.substring(line.indexOf('\t')+1).split('\t')
  }
}
