package com.dahua.tag

import org.apache.spark.sql.Row


/**
 * 关键字标签制作
 */
object KeyWordTags extends TagTraits {
  override def makeTags(arrs: Any*): Map[String, Int] = {
    var resultMap: Map[String, Int] = Map[String, Int]()

    val lineRow: Row = arrs(0).asInstanceOf[Row]
    val keyWords: String = lineRow.getAs[String]("keywords")

    //从广播变量中获取停用关键字 Map
    val broadcast: Map[String, String] = arrs(1).asInstanceOf[Map[String, String]]

    //|分割的关键字被看做多个关键字 转化为多个关键字标签
    if (keyWords == null) {
      resultMap += "K未知" -> 1
    } else {
      keyWords.split("[|]", -1)
        //关键字不能包含停用的关键字
        .filter(x => x.length >= 3 && x.length <= 8 && !broadcast.contains(x))
        .foreach(x => resultMap += "K" + x -> 1)
    }


    resultMap

  }
}
