# kafka-sparkstreaming-hive
sparkstreaming读取kafka数据到hive，offset由Redis集群维护，实现精准消费

提交参数
spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 10 \
--executor-memory 1G \
--executor-cores 1 \
--driver-memory 4g  \
--files /etc/spark2/2.6.4.0-91/0/hive-site.xml \
--class com.sparkstreaming.kafka2hive.KafkaSparkStreamingRedis \
kafka2hive.jar "ip:9092,ip:9092,ip:9092"  topic  Consumergroup  hiveDB hivetable 

注意：--num-executors 的个数根据topic的分区数指定

实践中遇到的难点
在部署的过程中遇到了偏移量outofrang的错误，对offset做了偏移量的矫正，具体矫正过程是先获取kafka中的offset的fromOffset和untilOffset偏移量，与Redis中的偏移量对比，具体详细比较过程请参考代码
