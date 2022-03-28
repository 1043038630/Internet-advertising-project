package com.dahua.tag

import ch.hsr.geohash.GeoHash
import com.dahua.tools.BaiDuRegionAPI
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
 * 经纬度 标签
 */
object JingWeiTags {


  /**
   * 判断Redis中有没有当前经纬的地区
   *
   * @param jedis
   * @param jing
   * @param wei
   */
  def judgeJingWei(jedis: Jedis, row: Row): Boolean = {
    val jing: String = row.getAs[String]("long")
    val wei: String = row.getAs[String]("lat")
    var base3: String = ""
    if (StringUtils.isNotEmpty(jing) && StringUtils.isNotEmpty(wei)) {
      base3 = GeoHash.withCharacterPrecision(wei.toDouble, jing.toDouble, 8).toBase32
      println("加密内容:" + base3)
    }
    if (StringUtils.isNotEmpty(jedis.get(base3))) {
      true
    } else {
      false
    }
  }

  /**
   * 向Redis中添加
   *
   * @param jedis
   * @param jing
   * @param wei
   */
  def addRedis(jedis: Jedis, row: Row) = {
    val jing: String = row.getAs[String]("long")
    val wei: String = row.getAs[String]("lat")
    var business: String = ""

    //判断经纬长度 如果不合法就没必要访问百度了
    if (StringUtils.isNotEmpty(wei) && StringUtils.isNotEmpty(jing)
      && wei.toDouble > 3 && wei.toDouble < 54 && jing.toDouble > 73 && jing.toDouble < 136) {
      //读取百度
      business = BaiDuRegionAPI.getBusiness(wei + "," + jing)
      if (StringUtils.isNotEmpty(business)) {
        val base3: String = GeoHash.withCharacterPrecision(wei.toDouble, jing.toDouble, 8).toBase32

        jedis.set(base3, business)
      }
    }
    business
  }

  def readReids(jedis: Jedis, row: Row) = {
    val jing: String = row.getAs[String]("long")
    val wei: String = row.getAs[String]("lat")
    val base3: String = GeoHash.withCharacterPrecision(wei.toDouble, jing.toDouble, 8).toBase32
    jedis.get(base3)
  }

  def makeTags(arrs: Any*) = {
    var str: String = arrs(0).asInstanceOf[String]

    var resultMap: Map[String, Int] = Map[String, Int]()

    resultMap += "Q" + str -> 1

    resultMap

  }


}
