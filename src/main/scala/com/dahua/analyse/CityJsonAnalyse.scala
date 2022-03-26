package com.dahua.analyse

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 将省市 统计 写入到磁盘，要根据省市分文件，格式：json
 */
object CityJsonAnalyse {

  def main(args: Array[String]): Unit = {

    //教研参数
    if (args.length != 2) {
      println("参数错误")
      sys.exit()
    }

    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("CityJsonAnalyse").getOrCreate()

    val dataFrame: DataFrame = spark.read.parquet(args(0))

    //设置临时表名称
    dataFrame.createTempView("tmp")

    val resultDataFrame: DataFrame = spark.sql("select provincename,cityname,count(1) tongji from tmp group by provincename,cityname order by provincename,cityname")

    //将结果dataframe写入到磁盘 使用json格式，并且根据省市分区,向分成一个区，在根据省市分区
    resultDataFrame.coalesce(1).write.partitionBy("provincename", "cityname").json(args(1))

    spark.stop()
  }
}
