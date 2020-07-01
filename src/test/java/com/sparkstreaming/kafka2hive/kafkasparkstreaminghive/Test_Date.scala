package com.sparkstreaming.kafka2hive.kafkasparkstreaminghive

import java.text.SimpleDateFormat
import java.util.Date

object Test_Date {
  def main(args: Array[String]): Unit = {
    print(new SimpleDateFormat("yyyy-MM-dd").format(new Date).toString)
  }
}
