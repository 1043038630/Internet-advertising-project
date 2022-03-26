package com.dahua.tools


/**
 * 测试切分
 *
 *
 */
object TestSplit {

  def main(args: Array[String]): Unit = {
    var str: String = "abcdefg";

    println(str.substring(2))
  }

  case class HuLianWang(sessionid: String,
                        advertisersid: Int,
                        adorderid: Int
                       )
}


