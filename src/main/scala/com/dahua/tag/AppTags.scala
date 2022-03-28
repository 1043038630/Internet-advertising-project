package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * APP名称以及渠道id 标签制作
 */
object AppTags extends TagTraits {
  override def makeTags(arrs: Any*): Map[String, Int] = {
    var resultMap: Map[String, Int] = Map[String, Int]()

    val lineRow: Row = arrs(0).asInstanceOf[Row]
    //从广播变量中获取的map
    val appNameMapping: Map[String, String] = arrs(1).asInstanceOf[Map[String, String]]

    val appName: String = lineRow.getAs[String]("appname")
    val appId: String = lineRow.getAs[String]("appid")

    val adplatformproviderid: Int = lineRow.getAs[Int]("adplatformproviderid")

    if (StringUtils.isEmpty(appName)) {
      resultMap += "APP " + appNameMapping.getOrElse(appId, "未知appName") -> 1
    } else {
      resultMap += "APP " + appName -> 1
    }
    //渠道标签制作
    resultMap += "CN " + adplatformproviderid -> 1


    resultMap
  }
}
