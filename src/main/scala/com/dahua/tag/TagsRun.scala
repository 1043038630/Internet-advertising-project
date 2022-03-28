package com.dahua.tag

import com.dahua.utils.TagUserId
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
 * 运行用户标签
 */
object TagsRun {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |参数错误
          |""".stripMargin)

      sys.exit()
    }

    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val spark: SparkSession = SparkSession.builder().config(conf).master("local[*]").appName("TagsRun").getOrCreate()


    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    var Array(logFile, appMappingFile, stopWordsFile, outputPath) = args

    //读取appname映射文件放入广播变量中
    val appNameMappingMap: Map[String, String] = sc.textFile(appMappingFile).map(x => {
      val split: Array[String] = x.split("[:]", -1)
      (split(0), split(1))
    }).collect().toMap[String, String]

    val appNameBroadcast: Broadcast[Map[String, String]] = sc.broadcast(appNameMappingMap)

    //停用词
    val stopWordsMap: Map[String, String] = sc.textFile(stopWordsFile).map((_, "")).collect().toMap[String, String]

    val stopWordsBroadcast: Broadcast[Map[String, String]] = sc.broadcast(stopWordsMap)

    //读取parquet文件
    val dataSet: Dataset[(String, List[(String, Int)])] = spark.read.parquet(logFile).where(TagUserId.userIdFilterParam).map(row => {
      // 广告标签
      val adsMap: Map[String, Int] = AdsTags.makeTags(row)
      // app标签.
      val appMap: Map[String, Int] = AppTags.makeTags(row, appNameBroadcast.value)
      // 驱动标签
      val driverMap: Map[String, Int] = DriverTags.makeTags(row)
      // 关键字标签
      val keyMap: Map[String, Int] = KeyWordTags.makeTags(row, stopWordsBroadcast.value)
      // 地域标签
      val pcMap: Map[String, Int] = RegionTags.makeTags(row)
      // 商圈标签.

      var userId: String = ""

      val userIdList: ListBuffer[String] = TagUserId.getUserId(row)
      //生成用户id
      if (userIdList.nonEmpty) {

        //可以再row中找到id
        userId = userIdList.head
      } else {
        //找不到 需要使用uuid补充
        userId = UUID.randomUUID().toString.substring(0, 6)
      }
      (userId, (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap).toList)
    })
    dataSet.rdd.reduceByKey((list1, list2) => {
      //将map根据key分组再将value加到一起 k1001 -> 1  k1002 -> 1
      (list1 ++ list2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
    }).saveAsTextFile(outputPath)

  }

}
