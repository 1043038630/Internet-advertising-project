package com.dahua.subject

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 统计各个省市分布情况，并排序。
 */
object Subject2 {
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


    //创建hadoop提供的文件系统对象
    val fileSystem: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    //判断文件系统中是否存在了当前文件
    if (fileSystem.exists(new Path(args(1)))) {
      //如果存在就删除
      fileSystem.delete(new Path(args(1)), true)
    }


    val dataFrame: DataFrame = spark.read.parquet(args(0))

    dataFrame.createTempView("tmp")

    spark.sql("select provincename,cityname,count(1) tongji from tmp group by provincename,cityname order by provincename,tongji").coalesce(1).write
      .json(args(1))

    spark.stop()
  }
}
