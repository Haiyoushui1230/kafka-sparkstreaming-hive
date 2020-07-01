package com.sparkstreaming.kafka2hive


import org.apache.spark.SparkConf
import org.apache.spark.sql.{ SparkSession}
object SparkSqlToHive {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
    val spark  = SparkSession.builder().appName("KafkaSparkStreamin").master("local[*]").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    spark.sql("use kafka_db")
    spark.sql("insert into table t1 (record,pt_date) values('tt','2019-02-09')")
//    spark.sql("select * from t1").show()
   spark.stop()
  }

}
