package com.dahua.analyse

import cn.hutool.core.io.resource.ClassPathResource
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties


/**
 * 将分析的数据放到mysql中  省市区分区
 */
object CityMysqlAnalyse {

  def main(args: Array[String]): Unit = {

    //教研参数
    if (args.length != 1) {
      println(
        """
          |参数错误，没有指明文件输入的路径
          |no input file path
          |""".stripMargin)

      sys.exit()
    }


    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("CityMysqlAnalyse").getOrCreate()

    val dataFrame: DataFrame = spark.read.parquet(args(0))

    dataFrame.createTempView("tmp")

    val resultDataFrame: DataFrame = spark.sql("select provincename,cityname,count(1) tongji from tmp group by provincename,cityname order by provincename,cityname")

    //使用properties工具类 直接读取类文件加下的所有properties文件
    val loadProperty: Config = ConfigFactory.load()

    //首先获取资源
    val resource = new ClassPathResource("jdbc.properties")
    //创建properties对象 加载这个资源
    val properties2 = new Properties();
    properties2.load(resource.getStream);
    val properties = new Properties()

    properties.setProperty("driver", properties2.get("jdbc.driver").toString)
    properties.setProperty("user", properties2.get("jdbc.user").toString)
    properties.setProperty("password", properties2.get("jdbc.password").toString)

    //写入到mysql数据库中
    resultDataFrame.write.mode(SaveMode.Overwrite).jdbc(properties2.get("jdbc.url").toString, properties2.get("jdbc.tableName").toString, properties)

    spark.stop()
  }
}
