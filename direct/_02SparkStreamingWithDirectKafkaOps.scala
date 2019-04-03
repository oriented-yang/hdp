package com.desheng.bigdata.spark.scala.streaming.p2.direct

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.{JavaConversions, mutable}

/**
  * 基于Direct模式来处理kafka数据
  * 需要我们自己来管理偏移量--过程
  * 1、从zk中获取对应topic的偏移量
  *     有可能有
  *     有可能无
  * 2、基于第一步中的偏移量信息，从kafka中读取数据
  *     有偏移量的方式：createDirectStream(ssc, kafkaParams, offsets,  messageHandler)
  *     无偏移量的方式：createDirectStream(ssc, kafkaParams, topics)
  * 3、基于第二步中读取到的数据，进行业务处理
  *
  * 4、基于第三步，处理完毕之后将数据更新会zk
  *
  */
object _02SparkStreamingWithDirectKafkaOps {
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

        val messages:InputDStream[(String, String)] = createMsg(ssc, kafkaParams, topics, groupId)
        //step 3、业务处理
        messages.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("-------------------------------------------")
                println(s"Time: $bTime")
                println("-------------------------------------------")
                println("########rdd'count: " + rdd.count())
                //step 4、更新偏移量
                store(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupId)
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
    def createMsg(ssc:StreamingContext, kafkaParams:Map[String, String],
                  topics:Set[String], group:String):InputDStream[(String, String)] = {
        //step 1、从zk中获取偏移量
        val offsets: Map[TopicAndPartition, Long] = getOffsets(topics, group)
        var messages:InputDStream[(String, String)] = null
        //step 2、基于偏移量创建message
        if(!offsets.isEmpty) {//读取到了对应的偏移量
            val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
            messages = KafkaUtils.createDirectStream[String, String,
                            StringDecoder, StringDecoder,
                            (String, String)](ssc,
                            kafkaParams, offsets, messageHandler)
        } else {//无偏移量
            messages = KafkaUtils.createDirectStream[
                        String, String,
                        StringDecoder, StringDecoder](
                            ssc, kafkaParams, topics)
        }
        messages
    }

    /**
      * step 1、获取对应topic的partition在zk中的偏移量信息
      * 框架默认将数据保存的路径：/kafka/consumers/${groupId}/offsets/${topic}/${partition}
      *                                                                          数据是offset
      * 自己模拟一个路径：
      *     /kafka/mykafka/offsets/${topic}/${groupId}/${partition}
      *                                                     数据是offset
      */

    def getOffsets(topics:Set[String], group:String): Map[TopicAndPartition, Long] = {
        val offsets = mutable.Map[TopicAndPartition, Long]()
        for (topic <- topics) {
            val path = s"${topic}/${group}"

            checkExists(path)

            for(partition <- JavaConversions.asScalaBuffer(curator.getChildren.forPath(path))) {
                val fullPath = s"${path}/${partition}"
                //获取指定分区的偏移量
                val offset = new String(curator.getData.forPath(fullPath)).toLong
                offsets.put(TopicAndPartition(topic, partition.toInt), offset)
            }
        }
        offsets.toMap
    }

    //step 4、更新偏移量
    def store(offsetRanges: Array[OffsetRange], group:String) {
        for(offsetRange <- offsetRanges) {
            val topic = offsetRange.topic
            val partition = offsetRange.partition
            val offset = offsetRange.untilOffset
            val fullPath = s"${topic}/${group}/${partition}"

            checkExists(fullPath)

            curator.setData().forPath(fullPath, (offset + "").getBytes())
        }
    }

    def checkExists(path:String): Unit = {
        if(curator.checkExists().forPath(path) == null) {
            curator.create().creatingParentsIfNeeded().forPath(path)//路径一定存在
        }
    }
    val curator = {
        val zk = "bigdata01:2181,bigdata02:2181,bigdata03:2181"
        val curator:CuratorFramework = CuratorFrameworkFactory
            .builder()
            .connectString(zk)
            .namespace("kafka/mykafka/offsets")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .build()
        curator.start()
        curator
    }
}
