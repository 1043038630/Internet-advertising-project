package com.dahua.tag

import com.dahua.tools.BaiDuRegionAPI
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
 * 地域标签 制作
 */
object RegionTags extends TagTraits {
  override def makeTags(arrs: Any*): Map[String, Int] = {
    var resultMap: Map[String, Int] = Map[String, Int]()

    val lineRow: Row = arrs(0).asInstanceOf[Row]
    val provincename: String = lineRow.getAs[String]("provincename")
    val cityname: String = lineRow.getAs[String]("cityname")

    val jing: String = lineRow.getAs[String]("long")
    val wei: String = lineRow.getAs[String]("lat")

    if (StringUtils.isNotEmpty(provincename)) {
      resultMap += "ZP" + provincename -> 1
    }
    if (StringUtils.isNotEmpty(cityname)) {
      resultMap += "ZC" + cityname -> 1
    }

    //调用百度逆位API算出经纬附近的商圈
    var str = wei + "," + jing
    if (wei.toDouble > 3 && wei.toDouble < 54 && jing.toDouble > 73 && jing.toDouble < 136) {
      println(str)
      val quan: String = BaiDuRegionAPI.getBusiness(str)
      resultMap += "Q" + quan -> 1
    }
    resultMap
  }
}
