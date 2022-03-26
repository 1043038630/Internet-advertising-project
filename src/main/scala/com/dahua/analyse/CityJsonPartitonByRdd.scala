package com.dahua.analyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 省市分区 根据RDD分区
 */
object CityJsonPartitonByRdd {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      """
        |参数错误
        |path exception
        |""".stripMargin

      sys.exit()
    }


    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf).master("local[1]").appName("CityJsonPartitonByRdd").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val fileRdd: RDD[String] = sc.textFile(args(0))
    fileRdd.map(x => {
      val split: Array[String] = x.split(",", -1)

      ((split(24), split(25)), 1)
    }).reduceByKey(_ + _)
      //现根据省排序 再根据市排序
      .sortBy(_._1._1).sortBy(_._1._2).foreach(println)

    sc.stop()

    spark.stop()

  }

}
