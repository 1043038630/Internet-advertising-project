package com.dahua.tools

import com.dahua.utils.RedisConnect
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis


/**
 * appname映射文件写入Redis缓存
 */
object AppMappings2Redis {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println(
        """
          |参数错误
          |""".stripMargin)
      sys.exit()
    }

    //验证结束
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("ZoneDimByRdd").getOrCreate()

    //使用spark对象读取parquet文件
    val sc: SparkContext = spark.sparkContext

    val fileRdd: RDD[String] = sc.textFile(args(0))

    fileRdd.map(x => {
      val split: Array[String] = x.split("[:]", -1)
      (split(0), split(1))
    }).foreachPartition(ite => {
      //一个分区开启开启一次Redis链接
      val jedis: Jedis = RedisConnect.getJedis
      ite.foreach(x => {
        jedis.set(x._1, x._2)
      })
      //关闭Redis链接
      jedis.close()
    })
    println("程序运行结束")
  }


}
