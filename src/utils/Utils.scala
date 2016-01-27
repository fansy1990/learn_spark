package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * Created by Fansy on 2016/1/26.
 */
class Utils {

}
object Utils{
  def getHadoopConf ={
    val conf:Configuration = new Configuration()
    conf.set("","")
    conf.set("mapred.remote.os","Linux");
    conf.set("fs.defaultFS", "node88:8020");// 指定namenode
    conf.set("mapreduce.framework.name","yarn"); // 指定使用yarn框架
    conf.set("yarn.resourcemanager.address","node88:8032"); // 指定resourcemanager
//    conf.set("yarn.resourcemanager.scheduler.address", SCHEDULER);// 指定资源分配器
//    conf.set("mapreduce.jobhistory.address", JOBHISTORY);
    conf
  }
  def getFs={
    FileSystem.get(getHadoopConf)
  }
}
