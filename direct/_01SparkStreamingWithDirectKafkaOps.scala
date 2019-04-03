package com.desheng.bigdata.spark.scala.streaming.p2.direct

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于Direct模式来处理kafka数据,这里没有在zookeeper中维护偏移量
  */
object _01SparkStreamingWithDirectKafkaOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        if(args == null || args.length < 3) {
            println(
                """Parameter Errors ! Usage: <batchInterval> <groupId> <topicList>
                  |batchInterval    ：  作业提交的间隔时间
                  |groupId          ：  分组id
                  |topicList        ：  要消费的topic列表
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, groupId, topicList) = args

        val conf = new SparkConf()
                    .setAppName("SparkStreamingWithDirectKafkaOps")
                    .setMaster("local[*]")

        val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "group.id" -> groupId,
            //largest从偏移量最新的位置开始读取数据
            //smallest从偏移量最早的位置开始读取
            "auto.offset.reset" -> "smallest"
        )
        val topics = topicList.split(",").toSet
        //基于Direct的方式读取数据
        val kafkaDStream:InputDStream[(String, String)] = KafkaUtils
            .createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, topics)

        kafkaDStream.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("-------------------------------------------")
                println(s"Time: $bTime")
                println("-------------------------------------------")
                rdd.foreach{case (key, value) => {
                    println(value)
                }}
                //查看rdd的范围
                println("偏移量范围：")
                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                for (offsetRange <- offsetRanges) {
                    val topic = offsetRange.topic
                    val parition = offsetRange.partition
                    val fromOffset = offsetRange.fromOffset
                    val untilOffset = offsetRange.untilOffset
                    val count = offsetRange.count()
                    println(s"topic:${topic}, partition:${parition}, " +
                        s"fromOffset:${fromOffset}, untilOffset:${untilOffset}, count:${count}")
                }
            }
        })
        ssc.start()
        ssc.awaitTermination()
    }
}
