package com.dahua.dim

import com.dahua.bean.LogBean
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 媒体指标数据清洗 根据RDD清洗
 * 有两个文件，一个是要分析的数据，但是有的appname 是空的，我们需要将空的补上，于是需要另外一个比对的文件，根据对比的文件将null的替换为
 */
object MediaDimByRdd {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println(
        """
          |参数错误
          |""".stripMargin)
      sys.exit()
    }

    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("MediaEtlByRdd").getOrCreate()


    val sc: SparkContext = spark.sparkContext

    //读取比对的文件


    //将文件读取每一行的内容根据冒号剪切并且分割成KV形式 变成map集合
    val filtMap: Map[String, String] = sc.textFile(args(0)).map(x => {
      val split: Array[String] = x.split("[:]", -1)
      (split(0), split(1))
    }).collect().toMap

    //将map放入广播变量中
    val broMap: Broadcast[Map[String, String]] = sc.broadcast(filtMap)

    //读取日志文件过去appid是空的
    val logBeanRdd: RDD[LogBean] = sc.textFile(args(1)).map(x => {
      x.split("[,]", -1)
    }).filter(x => x.length >= 85).map(LogBean(_)).filter(x => {
      x.appid.nonEmpty
    })

    //此时的rdd的appid都不是空的了
    logBeanRdd.map(x => {
      //如果appname是null的
      if (x.appname.equals("") || x.appname.isEmpty) {
        //根据appid在广播比变量找到对应的appname如果没有就是用默认的
        val appName: String = broMap.value.getOrElse(x.appid, "未知appname")
        //将空的替换
        x.appname = appName
      }
      //此时appname就不为空了

      //调用指标判断方法
      val qingqiu: List[Double] = ZoneDimJudge.qingQiuJudge(x.requestmode, x.processnode)
      val canYu: List[Double] = ZoneDimJudge.canYuJingJiaJudge(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.adorderid)
      val chengGong: List[Double] = ZoneDimJudge.chengGongJingJiaJudge(x.adplatformproviderid, x.iseffective, x.isbilling, x.isbid, x.iswin)
      val guangGao: List[Double] = ZoneDimJudge.guangGaoZhanShi(x.requestmode, x.iseffective)
      val meiJie: List[Double] = ZoneDimJudge.meiJieZhanShi(x.requestmode, x.iseffective, x.isbilling)
      val dsp: List[Double] = ZoneDimJudge.DSPXiaoFei(x.adplatformproviderid, x.iseffective, x.isbilling, x.iswin, x.adorderid, x.adcreativeid, x.winprice, x.adpayment)
      ((x.appname), qingqiu ++ canYu ++ chengGong ++ guangGao ++ meiJie ++ dsp)
    })
      //将他们聚合
      .reduceByKey((x, y) => {
        //将响铃的两个集合融合，两个集合对应相加
        x.zip(y).map(z => {
          z._1 + z._2
        })
      }).saveAsTextFile(args(2))

    spark.stop()

  }

}
