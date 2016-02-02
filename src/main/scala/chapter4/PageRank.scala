package chapter4

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import utils.Utils

/**
 * Created by fansy on 2016/2/2.
 */
object PageRank extends App{
  val sc = Utils.getSparkConf("pagerank")
  val file = Utils.HDFS+"/user/fansy/pk_obj.dat"
  val output = Utils.HDFS+"/user/fansy/pk_out"
  Utils.delHDFS(output,true)
  Utils.delHDFS(output+"_sorted",true)
  val links = sc.objectFile[(String, Seq[String])](file)
//    .partitionBy(new HashPartitioner(100))
//    .persist()
  //
  var ranks:RDD[(String, Double)] = links.mapValues(v=>1.0)

  // run 10 iterations
  for (i <- 0 until 10) {
    val contributions = links.join(ranks).flatMap {
      case (pageId, (links, rank)) =>
        links.map(dest => (dest, rank / links.size))
    }
    ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
  }
//  ranks.sortBy()

  ranks.saveAsTextFile(output)

  ranks.map(x=>(x._2,x._1)).sortByKey(false).saveAsTextFile(output+"_sorted")

  sc.stop()
}
