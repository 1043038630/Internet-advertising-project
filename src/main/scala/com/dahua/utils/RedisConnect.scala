package com.dahua.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
 * redis链接工具类
 */
object RedisConnect {

  val jedisPool = new JedisPool(new GenericObjectPoolConfig, "192.168.149.131", 6379, 30000, null, 4)

  def getJedis = jedisPool.getResource

  def main(args: Array[String]): Unit = {
    println(getJedis)
  }
}
