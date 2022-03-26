package com.dahua.utils

import com.dahua.tools.LogSchema
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/** \
 *
 * 日志转换parquet文件
 */
object Log2Parquet2 {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |参数不匹配
          |""".stripMargin)
      sys.exit()
    }

    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("Log2Parquet2").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    //DataFrame中是row对象，一个row对象代表一行，此对象有length方法可以判断一行的字段个数，因此可以过滤大于85个字段的内容
    val dataFrame: DataFrame = spark.read.format("csv").schema(LogSchema.structType).load(args(0)).filter(_.length >= 85)

    //dataFrame.show(200)

    //创建hadoop提供的文件系统对象
    val fileSystem: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    //判断文件系统中是否存在了当前文件
    if (fileSystem.exists(new Path(args(1)))) {
      //如果存在就删除
      fileSystem.delete(new Path(args(1)), true)
    }

    dataFrame.write.parquet(args(1))

    sc.stop()
    spark.stop()

  }

}
