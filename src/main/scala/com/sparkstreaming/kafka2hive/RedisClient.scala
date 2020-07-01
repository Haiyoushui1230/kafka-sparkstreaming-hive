package com.sparkstreaming.kafka2hive

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{JedisCluster, JedisPool}

object RedisClient {
  val redisHost = "rduft9ta.redis.db.cloud.papub"
  val redisPort = 10847
  val redisTimeout = 30000
  val password = "PowerData1101"
  /**
    * JedisPool是一个连接池，既可以保证线程安全，又可以保证了较高的效率。
    */

//  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout,password)
lazy val pool = new JedisPool(new GenericObjectPoolConfig(), "node01", 6379, redisTimeout)

}
