package com.dahua.subject

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable


/**
 * 使用RDD方式，完成按照省分区，省内有序。
 */
object Subject3 {


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      """
        |参数错误
        |path exception
        |""".stripMargin

      sys.exit()
    }
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf)
      .appName("CityJsonPartitonByRdd").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val fileRdd: RDD[String] = sc.textFile(args(0))

    val rdd2: RDD[((String, String), Int)] = fileRdd.map(x => {
      val split: Array[String] = x.split(",", -1)
      ((split(24), split(25)), 1)
    }).reduceByKey(_ + _).sortBy(x => x._2)

    //rdd进入缓存
    rdd2.cache()

    //执行排序
    rdd2.coalesce(1).partitionBy(MyPartition(32)).saveAsTextFile(args(1))

    sc.stop()

    spark.stop()
  }


  /**
   * 自定义分区 使用map存储  K：每个省份，V 每个省份的编号 省份编号从-1开始
   */
  case class MyPartition(parNum: Int) extends Partitioner {
    override def numPartitions: Int = parNum

    //省份的编号
    var index: Int = -1

    var maps = new mutable.HashMap[String, Int]()

    override def getPartition(key: Any): Int = {
      // ((split(24), split(25)), 1)
      val keyString: String = key.toString

      //将省份切出来
      val sheng: String = keyString.substring(1, keyString.indexOf(","))

      //判断省份是否在集合中 如果不在集合中 将省份当做key，给他一个编号，如果在集合中就获取这个编号
      if (maps.contains(sheng)) {
        //获取这个key的内容
        maps.getOrElse(sheng, index)
      } else {
        //不在集合中
        index += 1
        maps.put(sheng, index)
        //将index返回出去
        index
      }

    }
  }
}
