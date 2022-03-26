package com.dahua.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * 地域分析根据RDD实现
 */
object ZoneDimByRdd {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |参数错误
          |""".stripMargin)
      sys.exit()
    }

    //验证结束
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("ZoneDimByRdd").getOrCreate()

    import spark.implicits._

    //使用spark对象读取parquet文件
    val fileDataFrame: DataFrame = spark.read.parquet(args(0))

    val dimDataSet: Dataset[((String, String), List[Double])] = fileDataFrame.map(row => {
      //由于dataframe中的对象是row，根据row获取当前指标需要分析的字段
      val province: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
      val appname: String = row.getAs[String]("appname")
      val requestMode: Int = row.getAs[Int]("requestmode")
      val processNode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")
      val adcreativeid: Int = row.getAs[Int]("adcreativeid")


      //调用外部方法进行判断，判断逻辑根据项目指标来定,注意方法调用的顺序，很重要根据需求维度的顺序，并且有的方法是好多个维度公用的
      val qingQiu: List[Double] = ZoneDimJudge.qingQiuJudge(requestMode, processNode)
      val canYuJingJia: List[Double] = ZoneDimJudge.canYuJingJiaJudge(adplatformproviderid, iseffective, isbilling, isbid, adorderid)
      val chengGongJingJia: List[Double] = ZoneDimJudge.chengGongJingJiaJudge(adplatformproviderid, iseffective, isbilling, isbid, iswin)
      val guangGao: List[Double] = ZoneDimJudge.guangGaoZhanShi(requestMode, iseffective)
      val meiJie: List[Double] = ZoneDimJudge.meiJieZhanShi(requestMode, iseffective, isbilling)
      val dsp: List[Double] = ZoneDimJudge.DSPXiaoFei(adplatformproviderid, iseffective, isbilling, iswin, adorderid, adcreativeid, winprice, adpayment)

      //开始格式刷结果
      ((province, cityname), qingQiu ++ canYuJingJia ++ chengGongJingJia ++ guangGao ++ meiJie ++ dsp)
    })

    dimDataSet.rdd.reduceByKey { case (x, y) => {
      //调用拉链方法 将相邻的两个集合拉到一起List[(double,double)]，然后将他们依次相加
      x.zip(y).map(x => x._1 + x._2)
    }
    }.foreach(println)

    spark.stop()
  }
}
