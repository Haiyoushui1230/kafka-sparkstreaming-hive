package com.sparkstreaming.kafka2hive

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}


object RedisCluster {
  private val poolConf = new JedisPoolConfig

 val redisTimeout = 30000
  val password = "PowerData1101"

  val nodes = new util.HashSet[HostAndPort]()

  nodes.add(new HostAndPort("rduft9ta.redis.db.cloud.papub",10847))
//  nodes.add(new HostAndPort("127.0.0.1",7001))
//  nodes.add(new HostAndPort("127.0.0.1",7002))
//  nodes.add(new HostAndPort("127.0.0.1",7003))
//  nodes.add(new HostAndPort("127.0.0.1",7004))
//  nodes.add(new HostAndPort("127.0.0.1",7005))
//  nodes.add(new HostAndPort("127.0.0.1",7006))

  //soTimeout 读取数据超时时间
  //max-attempts             最大的连接重试次数
  def getRedisCluster: JedisCluster = {
//      new JedisCluster(nodes,redisTimeout,30000,100,"PowerData1101",new GenericObjectPoolConfig())
      new JedisCluster(nodes,redisTimeout,30000,100,"PowerData1101",new GenericObjectPoolConfig())
  }

}