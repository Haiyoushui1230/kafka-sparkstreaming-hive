package com.sparkstreaming.kafka2hive

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.sparkstreaming.kafka2hive.utils.{ClusterPoolUtil, KafkaOffsetTool}
import kafka.common.TopicAndPartition
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.JavaConversions._


/**
 * 实现精准消费，offset维护在Redis中
 */
object KafkaSparkStreamingRedis {
  def main(args: Array[String]): Unit = {

    val brokers = args(0)
    val topic = args(1)
    val consumerGroup = args(2)
    val hive_db = args(3)
    val hive_table = args(4)

    val spark = SparkSession.builder()
      .appName("KafkaSparkStreaming")
      .enableHiveSupport()
      .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(5))
    //设置日志级别
    //    ssc.sparkContext.setLogLevel("Error")

    val kafkaParams = Map[String, Object](
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "bootstrap.servers" -> brokers,
      "group.id" -> consumerGroup,
      "fetch.message.max.bytes" -> "52428800",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "earliest"
    )

    //从Redis 中获取消费者offset
    val currentTopicOffset: collection.Map[String, String] = getOffSetFromRedis(topic)

    //初始读取到的topic offset:
    currentTopicOffset.foreach(x => {
      println(s" 初始读取到的offset: $x")
    })

    //转换成需要的类型
    val fromOffsets: Map[TopicAndPartition, Long] = currentTopicOffset.map { resultSet =>
      TopicAndPartition(topic, resultSet._1.toInt) -> resultSet._2.toLong
    }.toMap

    val topics = List(topic)


    // lastest offsets
    val lastestTopicAndPartitionLongMap = KafkaOffsetTool.getInstance().getLastOffset(brokers, topics, consumerGroup)
    // earliest offsets
    val earliestTopicAndPartitionLongMap = KafkaOffsetTool.getInstance().getEarliestOffset(brokers, topics, consumerGroup);

    /**
      * 矫正过程
      */
    val correctFromOffset = earliestTopicAndPartitionLongMap.map(earliestTopicAndPartitionLongEntry => {
      val key = earliestTopicAndPartitionLongEntry._1   //TopicAndPartition
      val earliestOffset = earliestTopicAndPartitionLongEntry._2  //offset

      val redisOffset = fromOffsets.getOrDefault(key, 0L)
      val lastestOffset = lastestTopicAndPartitionLongMap.getOrDefault(key, 0L)

      if (redisOffset > lastestOffset || redisOffset < earliestOffset) {
        (key, earliestOffset)
      }else{
        (key, redisOffset)
      }
    })


    /**
     * 矫正后的偏移量
     */
    val correctOffset = correctFromOffset.map(o => {
      (new TopicPartition(o._1.topic, o._1.partition), o._2.asInstanceOf[Long])
    })

    /**
     * 将获取到的消费者offset 传递给SparkStreaming
     */

    val kafkaDSteam = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Set(topic), kafkaParams, correctOffset)
    )

    val structType: StructType = StructType(List[StructField](
      StructField("record", StringType, nullable = true),
      StructField("pd", StringType, nullable = true)
    ))

    kafkaDSteam.foreachRDD { rdd =>
      val dataValue = rdd.map(rdd => {
        val key_value = (rdd.key(), rdd.value())
//        println("key =:" + key_value._1)
//        println("value =" + key_value._2)
        key_value._2
      })

      val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date)

      val rowRdd = dataValue.map(a => {
        val row = Row(a, date)
         row
        }
      )

      val frame: DataFrame = spark.createDataFrame(rowRdd, structType)

      frame.createOrReplaceTempView("t2")

      spark.sql("use " + hive_db)

      val insertSql = s"insert into ${hive_db}.${hive_table} partition(pd='${date}') select record from t2"

      println(insertSql)

      spark.sql(insertSql)
      println("**** 业务处理完成，开始更新offset  ****")

      //获取kafka偏移量
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"topic:${o.topic}  partition:${o.partition}  fromOffset:${o.fromOffset}  untilOffset: ${o.untilOffset}")
      }
      //将当前批次最后的所有分区offsets 保存到 Redis中
      saveOffsetToRedis(offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  /**
   * 将消费者offset 保存到 Redis中
   *
   */
  def saveOffsetToRedis(offsetRanges: Array[OffsetRange]) = {
    val jedisCluster =ClusterPoolUtil.getJedisCluster
//    val jedisCluster = RedisCluster.getRedisCluster
    offsetRanges.foreach(one => {
      jedisCluster.hset(one.topic, one.partition.toString, one.untilOffset.toString)
    })
//     jedisCluster.close()
  }


  /**
   * 从Redis中获取保存的消费者offset
   *
   * @param topic
   * @return
   */
  def getOffSetFromRedis(topic: String) = {
    val jedisCluster =ClusterPoolUtil.getJedisCluster
//    val jedisCluster = RedisCluster.getRedisCluster
    val result: util.Map[String, String] = jedisCluster.hgetAll(topic)
//    jedisCluster.close()
    if(result == null){
      Map[String, String]()
    }else{
      val offsetMap: scala.collection.mutable.Map[String, String] = result
      offsetMap
    }
  }


}
