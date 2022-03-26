package com.dahua.utils

import com.dahua.tools.NumberFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 日志变成parquet文件
 */
object Log2Parquet {

  def main(args: Array[String]): Unit = {
    //判断参数是否正确
    if (args.length != 2) {
      """
        |缺少参数
        |inputPath outPath
        |""".stripMargin

      //退出程序
      sys.exit()
    }

    //创建spark对象
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("Log2Parquet").getOrCreate()

    //常见spark上下文对象 为了创建rdd
    val sc: SparkContext = spark.sparkContext

    val fileRdd: RDD[String] = sc.textFile(args(0))

    //逗号的意思是一直查询到最后一位，由于有些,间隔是null的 会将逗号看成一个值
    val rdd2: RDD[Row] = fileRdd.map(_.split(",", -1)).filter(_.length >= 85).map(arr => {
      //创建row对象 由于是最后一行 会自动返回
      Row(
        arr(0),
        NumberFormat.toInt(arr(1)),
        NumberFormat.toInt(arr(2)),
        NumberFormat.toInt(arr(3)),
        NumberFormat.toInt(arr(4)),
        arr(5),
        arr(6),
        NumberFormat.toInt(arr(7)),
        NumberFormat.toInt(arr(8)),
        NumberFormat.toDouble(arr(9)),
        NumberFormat.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NumberFormat.toInt(arr(17)),
        arr(18),
        arr(19),
        NumberFormat.toInt(arr(20)),
        NumberFormat.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NumberFormat.toInt(arr(26)),
        arr(27),
        NumberFormat.toInt(arr(28)),
        arr(29),
        NumberFormat.toInt(arr(30)),
        NumberFormat.toInt(arr(31)),
        NumberFormat.toInt(arr(32)),
        arr(33),
        NumberFormat.toInt(arr(34)),
        NumberFormat.toInt(arr(35)),
        NumberFormat.toInt(arr(36)),
        arr(37),
        NumberFormat.toInt(arr(38)),
        NumberFormat.toInt(arr(39)),
        NumberFormat.toDouble(arr(40)),
        NumberFormat.toDouble(arr(41)),
        NumberFormat.toInt(arr(42)),
        arr(43),
        NumberFormat.toDouble(arr(44)),
        NumberFormat.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NumberFormat.toInt(arr(57)),
        NumberFormat.toDouble(arr(58)),
        NumberFormat.toInt(arr(59)),
        NumberFormat.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NumberFormat.toInt(arr(73)),
        NumberFormat.toDouble(arr(74)),
        NumberFormat.toDouble(arr(75)),
        NumberFormat.toDouble(arr(76)),
        NumberFormat.toDouble(arr(77)),
        NumberFormat.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NumberFormat.toInt(arr(84))
      )
    })
    //由于将一行数据分割成了row对象，并且进行了类型转换，但是还不知道列名是什么

  }
}
