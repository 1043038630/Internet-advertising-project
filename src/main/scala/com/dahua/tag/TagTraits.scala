package com.dahua.tag

/**
 * 标签特质，制作标签类的父类
 */
trait TagTraits {

  def makeTags(arrs: Any*): Map[String, Int]

}
