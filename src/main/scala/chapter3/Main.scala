package chapter3

import utils.Utils

/**
 * Created by fansy on 2016/2/1.
 */
object Main  extends App{
  val sc = Utils.getSparkConf("test app")
  sc.setLogLevel("ERROR")
  val rdd1 = sc.parallelize(List("coffee","coffee","panda","monkey","tea"))
  val rdd2 = sc.parallelize(List("coffee","monkey","kitty"))

  rdd1.distinct().foreach(println)
  println(".....................");
  rdd1.union(rdd2).foreach(println)

  println(rdd1.intersection(rdd2))
  println(rdd1.subtract(rdd2))


  sc.stop()

}
