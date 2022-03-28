package com.dahua.tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
 * 广告位类型 标签制作
 */
object AdsTags extends TagTraits {


  override def makeTags(arrs: Any*): Map[String, Int] = {
    var resultMap = Map[String, Int]()

    //第一个参数传入row类型,判断是否是row如果是就变成row
    val lineRow: Row = arrs(0).asInstanceOf[Row]
    val asdInt: Int = lineRow.getAs[Int]("adspacetype")
    //如果是两位数
    if (asdInt > 9) {
      resultMap += "LC" + asdInt -> 1
    } else {
      //不足两位数补0
      resultMap += "LC0" + asdInt -> 1
    }
    //获取广告位类型
    val asdNameFssInt: String = lineRow.getAs[String]("adspacetypename")
    if (StringUtils.isNotEmpty(asdNameFssInt)) {
      resultMap += "LN" + asdNameFssInt -> 1
    } else {
      resultMap += "LN未知类型" -> 1
    }
    resultMap
  }
}
