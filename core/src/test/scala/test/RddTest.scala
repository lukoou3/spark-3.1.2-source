package org.apache.spark.rdd

import java.util

import org.apache.spark.{SparkConf, SparkContext}

object RddTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("appName").setMaster("master")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("")
    rdd.map((_, 1)).mapValues(_ + 1).keys.max()
    rdd.map((_, 1)).sortBy(_._1).repartition(2)
    rdd.map((_, 1)).sortByKey().repartition(6)
    util.Arrays.asList(1, 2, 4).forEach(println(_))
    val rsts: Array[(String, Int)] = rdd.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
    println(rsts.toBuffer)

    rdd.toJavaRDD().mapPartitions()

    sc.stop()
  }

}
