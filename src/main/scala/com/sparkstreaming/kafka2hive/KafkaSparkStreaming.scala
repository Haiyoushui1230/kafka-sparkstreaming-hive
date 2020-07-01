package com.sparkstreaming.kafka2hive



import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * kafla自己维护offset，异步更新
  */
object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并初始化SSC
    val spark  = SparkSession.builder().appName("KafkaSparkStreaming").enableHiveSupport().getOrCreate()
//    val spark  = SparkSession.builder().appName("KafkaSparkStreaming").master("local[*]").enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    //设置日志级别
    ssc.sparkContext.setLogLevel("Error")
    //2.定义kafka参数
//    val brokers = "node01:9092,node02:9092,node03:9092"
    val brokers = "100.68.97.1:9092;100.68.97.2:9092;100.68.97.3:9092"//东哥
    val topic = "T_POWER_SDK"
    val consumerGroup = "CID_POWER_SDK"
//    val pt_date = args(0)
    //3.将kafka参数映射为map
    val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )
    //4.通过KafkaUtil创建kafkaDSteam,使用direct模式
    val kafkaDSteam = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,//消费策略，这种策略会将分区均匀的分布在集群的Executor之间。
      Subscribe[String, String](Set(topic), kafkaParam)
    )
    import spark.implicits._

    val structType: StructType = StructType(List[StructField](
      StructField("record", StringType, nullable = true),
      StructField("pd", StringType, nullable = true)
    ))

    val transStrem = kafkaDSteam.map(rdd => {
      val key_value = (rdd.key(), rdd.value())
      println("key =:" + key_value._1)
      println("value =" + key_value._2)
      key_value._2
    })

    transStrem.foreachRDD(one=>{
      val rdd : RDD[Row] = one.map({
         a=>
          Row(a.toString,(new SimpleDateFormat("yyyy-MM-dd").format(new Date)).toString)
      })
      rdd.count()
      val frame : DataFrame = spark.createDataFrame(rdd,structType)
      frame.createOrReplaceTempView("t2")
      spark.sql("use kafka_test")
//      spark.sql("create external  table if not exists kafka2hvie(record   string) partitioned by (pd string) row format delimited fields terminated by '\\t' stored as Parquet location '/user/futongshuai356/data'")
      frame.write.format("parquet").mode(SaveMode.Append).saveAsTable("kafka2hive")
    }
    )

    kafkaDSteam.foreachRDD { rdd =>
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaDSteam.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    //6.启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }
}
