package com.dahua.tag

import com.dahua.utils.{RedisConnect, TagUserId}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import java.util.UUID
import scala.collection.mutable.ListBuffer

/**
 * 标签制作2
 */
object TagsRun2 {

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

    //判断写入地址是否有文件 有删除
    val system: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    if (system.exists(new Path(outputPath))) {
      system.delete(new Path(outputPath), true)
    }


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
    val dataFrame: DataFrame = spark.read.parquet(logFile)

    dataFrame.mapPartitions(ite => {

      val jedis: Jedis = RedisConnect.getJedis
      val iterator: Iterator[(String, List[(String, Int)])] = ite.map(x => {
        val row: Row = x
        //商圈
        var business: String = ""
        //如果Redis中存在
        if (JingWeiTags.judgeJingWei(jedis, row)) {
          //直接读取Redis内容
          business = JingWeiTags.readReids(jedis, row)
        } else {
          //读取百度向Redis中写入 返回商圈
          business = JingWeiTags.addRedis(jedis, row)
        }
        println("地址：" + business)
        //调用标签制作
        val businessMap: Map[String, Int] = JingWeiTags.makeTags(business)
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
        ((userId), ((adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap ++ businessMap).toList))
      })
      jedis.close()
      iterator
    }).rdd.reduceByKey((list1, list2) => {
      val tuples: List[(String, Int)] = list1 ++ list2
      val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(_._1)
      stringToTuples.mapValues(_.foldLeft(0)(_ + _._2)).toList
    }).saveAsTextFile(outputPath)

  }

}
