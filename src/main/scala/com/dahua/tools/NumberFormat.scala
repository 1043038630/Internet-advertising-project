package com.dahua.tools


/**
 * 将字符串转换成数字
 */
object NumberFormat {

  def toInt(field: String) = {
    try {
      field.toInt
    } catch {
      case _: Exception => 0
    }
  }

  def toDouble(field: String) = {
    try {
      field.toDouble
    } catch {
      case _: Exception => 0
    }
  }
}
