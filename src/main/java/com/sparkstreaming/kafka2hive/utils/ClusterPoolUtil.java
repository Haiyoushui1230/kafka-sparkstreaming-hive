package com.sparkstreaming.kafka2hive.utils;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
public class ClusterPoolUtil {
    private static JedisCluster jedisCluster;

    public static JedisCluster getJedisCluster() {
        Properties props = new Properties();
        try {
            if (jedisCluster == null) {
                int connectTimeOut = 30000;//连接超时
                int soTimeOut = 5000;//读写超时
                int maxAttemts = 50;//重试次数
                Set jedisClusterNode = new HashSet();
                JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxTotal(Integer.valueOf(props.getProperty("redis.maxActive")));
                poolConfig.setMaxIdle(Integer.valueOf(props.getProperty("redis.maxIdle")));
                poolConfig.setMaxWaitMillis(Long.valueOf(props.getProperty("redis.maxWaitMillis")));
                poolConfig.setTestOnBorrow(Boolean.valueOf(props.getProperty("redis.testOnBorrow")));
                poolConfig.setTestOnReturn(Boolean.valueOf(props.getProperty("redis.testOnReturn")));
                jedisClusterNode.add(new HostAndPort("rduft9ta.redis.db.cloud.papub",10847));
                if (jedisCluster == null) {
                    jedisCluster = new JedisCluster(jedisClusterNode, connectTimeOut, soTimeOut, maxAttemts,"PowerData1101", poolConfig);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jedisCluster;
    }

    public static void set(String key, String value) {
        jedisCluster = getJedisCluster();
        jedisCluster.set(key, value);
    }

    public static String get(String key) {
        jedisCluster = getJedisCluster();
        String value = jedisCluster.get(key);
        return value;
    }

    public static void main(String[] args) {
        set("key1", "value1");
        System.out.println(get("key1"));
    }

}