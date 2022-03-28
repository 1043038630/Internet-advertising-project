package com.dahua.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer


/**
 * 用户画像标签 用户id生成的工具
 */
object TagUserId {


  /**
   * 用来做数据过滤的条件，以下的参数会当做userid 必须有一个不是空字符串的
   */
  val userIdFilterParam: String =
    """
      |imei != "" or imeimd5 != "" or imeisha1 != "" or
      |idfa != "" or idfamd5 != "" or idfasha1 != "" or
      |mac != "" or macmd5 != "" or macsha1 != "" or
      |androidid != "" or androididmd5 != "" or androididsha1 != "" or
      |openudid != "" or openudidmd5 != "" or openudidsha1 != ""
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    println(userIdFilterParam)
  }

  /**
   * 解析row对象来获取用户id
   *
   * @param row
   */
  def getUserId(row: Row) = {
    val userIdList: ListBuffer[String] = ListBuffer[String]()

    if (StringUtils.isNotEmpty(row.getAs[String]("imei"))) userIdList.append(row.getAs[String]("imei").toUpperCase())
    if (row.getAs[String]("imeimd5") != null && row.getAs[String]("imeimd5").nonEmpty) {
      userIdList.append("IMD:" + row.getAs[String]("imeimd5").toUpperCase)
    }
    if (row.getAs[String]("imeisha1") != null && row.getAs[String]("imeisha1").nonEmpty) {
      userIdList.append("IMS:" + row.getAs[String]("imeisha1").toUpperCase)
    }
    if (row.getAs[String]("idfa") != null && row.getAs[String]("idfa").nonEmpty) {
      userIdList.append("ID:" + row.getAs[String]("idfa").toUpperCase)
    }
    if (row.getAs[String]("idfamd5") != null && row.getAs[String]("idfamd5").nonEmpty) {
      userIdList.append("IDM:" + row.getAs[String]("idfamd5").toUpperCase)
    }
    if (row.getAs[String]("idfasha1") != null && row.getAs[String]("idfasha1").nonEmpty) {
      userIdList.append("IDS:" + row.getAs[String]("idfasha1").toUpperCase)
    }
    if (row.getAs[String]("mac") != null && row.getAs[String]("mac").nonEmpty) {
      userIdList.append("MAC:" + row.getAs[String]("mac").toUpperCase)
    }
    if (row.getAs[String]("macmd5") != null && row.getAs[String]("macmd5").nonEmpty) {
      userIdList.append("MACM:" + row.getAs[String]("macmd5").toUpperCase)
    }
    if (row.getAs[String]("macsha1") != null && row.getAs[String]("macsha1").nonEmpty) {
      userIdList.append("MACS:" + row.getAs[String]("macsha1").toUpperCase)
    }
    if (row.getAs[String]("androidid") != null && row.getAs[String]("androidid").nonEmpty) {
      userIdList.append("AD:" + row.getAs[String]("androidid").toUpperCase)
    }
    if (row.getAs[String]("androididmd5") != null && row.getAs[String]("androididmd5").nonEmpty) {
      userIdList.append("ADM:" + row.getAs[String]("androididmd5").toUpperCase)
    }
    if (row.getAs[String]("androididsha1") != null && row.getAs[String]("androididsha1").nonEmpty) {
      userIdList.append("IMS:" + row.getAs[String]("androididsha1").toUpperCase)
    }
    if (row.getAs[String]("openudid") != null && row.getAs[String]("openudid").nonEmpty) {
      userIdList.append("OP:" + row.getAs[String]("openudid").toUpperCase)
    }
    if (row.getAs[String]("openudidmd5") != null && row.getAs[String]("openudidmd5").nonEmpty) {
      userIdList.append("OPM:" + row.getAs[String]("openudidmd5").toUpperCase)
    }
    if (row.getAs[String]("openudidsha1") != null && row.getAs[String]("openudidsha1").nonEmpty) {
      userIdList.append("OPS:" + row.getAs[String]("openudidsha1").toUpperCase)
    }
    userIdList

  }


}
