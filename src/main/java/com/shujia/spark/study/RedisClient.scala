package com.shujia.spark.study

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisClient extends Serializable {
  val redisHost = "node3"
  val redisPort = 6379
  val redisTimeout = 30000
  //redis链接池
  //lazy 懒执行，用到的时候才会执行
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook: Thread = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  //当main方法执行完成之前调用
  sys.addShutdownHook(hook.run)
}