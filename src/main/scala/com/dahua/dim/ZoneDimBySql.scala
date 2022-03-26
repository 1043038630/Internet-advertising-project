package com.dahua.dim

import cn.hutool.core.io.resource.ClassPathResource
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties


/**
 * 地域多维度报表分析 根据sparksql
 * 读取parquet文件 使用sql分析
 */
object ZoneDimBySql {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println(
        """
          |
          |参数错误
          |""".stripMargin)

      sys.exit()
    }

    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("ZoneDimBySql").getOrCreate()

    val fileDataFrame: DataFrame = spark.read.parquet(args(0))

    fileDataFrame.createTempView("table1")

    val sql =
      """
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ysqq,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yxqq,
        |sum(case when requestmode = 1 and processnode >= 3 then 1 else 0 end) ggqq,
        |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and isbid = 1 and adorderid != 0  then 1 else 0 end) cyjj,
        |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1 and iswin = 1  then 1 else 0 end) jjcg,
        |sum(case when requestmode = 2 and iseffective = 1  then 1 else 0 end) zssggz,
        |sum(case when requestmode = 3 and iseffective = 1  then 1 else 0 end) djsggz,
        |sum(case when requestmode = 2 and iseffective = 1 and  isbilling = 1  then 1 else 0 end) zssmj,
        |sum(case when requestmode = 3 and iseffective = 1 and  isbilling = 1  then 1 else 0 end) djsmj,
        |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1
        |and iswin = 1 and adorderid >200000 and adcreativeid > 200000
        |then winprice/1000 else 0 end) DSPxiaofei,
        |sum(case when adplatformproviderid >= 100000 and iseffective = 1 and isbilling = 1
        |and iswin = 1 and adorderid >200000 and adcreativeid > 200000
        |then adpayment/1000 else 0 end) DSPchengben
        |from table1
        |group by provincename,cityname
        |""".stripMargin

    val resultDataFrame: DataFrame = spark.sql(sql)


    //读取properties文件获取jdbc信息
    //将读取的资源封装到properties中
    val properties = new Properties()
    properties.load(new ClassPathResource("jdbc.properties").getStream)

    val properties1 = new Properties()
    properties.setProperty("driver", properties.get("jdbc.driver").toString)
    properties.setProperty("user", properties.get("jdbc.user").toString)
    properties.setProperty("password", properties.get("jdbc.password").toString)

    resultDataFrame.write.jdbc(properties.get("jdbc.url").toString, properties.get("jdbc.tableName2").toString, properties1)

    spark.stop()

  }


}
