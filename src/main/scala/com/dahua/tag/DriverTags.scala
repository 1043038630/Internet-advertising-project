package com.dahua.tag

import org.apache.spark.sql.Row

/**
 * 驱动类标签制作，操作系统 联网方式 运行商
 */
object DriverTags extends TagTraits {
  override def makeTags(arrs: Any*): Map[String, Int] = {
    var resultMap: Map[String, Int] = Map[String, Int]()
    val lineRow: Row = arrs(0).asInstanceOf[Row]
    val clientId: Int = lineRow.getAs[Int]("client")
    //联网方式
    val networkmannername: String = lineRow.getAs[String]("networkmannername")

    //网络运行商获取
    val ispname: String = lineRow.getAs[String]("ispname").trim

    //使用模式匹配来判断
    clientId match {
      case 1 => resultMap += "D00010001" -> 1
      case 2 => resultMap += "D00010002" -> 1
      case 3 => resultMap += "D00010003" -> 1
      case _ => resultMap += "D00010004" -> 1
    }

    networkmannername match {
      case "WIFI " => resultMap += "D00020001 " -> 1
      case "4G  " => resultMap += "D00020002 " -> 1
      case "3G  " => resultMap += "D00020003 " -> 1
      case "2G  " => resultMap += "D00020004 " -> 1
      case _ => resultMap += "D00020005 " -> 1
    }

    ispname match {
      case "移动" => resultMap += "D00030001 " -> 1
      case "联通" => resultMap += "D00030002 " -> 1
      case "电信" => resultMap += "D00030003 " -> 1
      case _ => resultMap += "D00030004 " -> 1
    }

    resultMap

  }
}
