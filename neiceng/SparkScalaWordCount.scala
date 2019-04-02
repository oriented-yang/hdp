package com.desheng.bigdata.spark.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 在Spark中，所有的编程入口都是各种各样的Context
  *   在SparkCore中的入口：SparkContext
  *       有java和scala之分，java中是JavaSparkContext，scala是SparkContext
  *   在SparkSQL，
  *     spark2.0以前使用SQLContext，或者HiveContext
  *     spark2.0以后统一使用SparkSession的api
  *   在SparkStreaming中，使用StreamingContext作为入口
  *
  *   推荐大家的编程方式：倒推+填空（大体的逻辑架构）
  *
  * 编程步骤：
  *     1、构建SparkContext
  *         SparkContext需要一个SparkConf的依赖
  *             conf中必须要指定master url
  *             conf中必须要指定应用名称
  *     2、加载外部数据，形成一个RDD
  *     3、根据业务需要对RDD进行转换操作
  *     4、提交作业
  *     5、释放资源
  */
object SparkScalaWordCount {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        /**Master URL配置
          *   说明：Master URL代表的含义是Spark作业运行的方式
          *   本地模式(local) --Spark作业在本地运行(Spark Driver和Executor在本地运行)
          *     local       : 为当前的Spark作业分配一个工作线程
          *     local[M]    ：为当前的Spark作业分配M个工作线程
          *     local[*]    ：为当前的Spark作业分配当前计算集中所有可用的工作线程
          *     local[M, N] ：为当前的Spark作业分配M个工作线程，如果Spark作业提交失败，会进行最多N的重试
          *   基于spark自身集群(standalone)
          *     格式：spark://<masterIp>:<masterPort>
          *         比如:spark://bigdata01:7077
          *     HA格式： spark://<masterIp1>:<masterPort1>,<masterIp2>:<masterPort2>,...
          *         比如：spark://bigdata01:7077,bigdata02:7077
          *   基于yarn（国内主流）
          *        cluster: SparkContext或者Driver在yarn集群中进行创建
          *        client:  SparkContext或者Driver在本地创建(所谓本地就是提交作业的那一台机器)
          *   基于mesos(欧美主流)：
          *         master
          *         slave
          */
        val conf = new SparkConf()
        conf.setAppName(s"${SparkScalaWordCount.getClass.getSimpleName}")
        conf.setMaster("local[*]")
        val sc = new SparkContext(conf)
        //加载本地的文件
        val lines:RDD[String] = sc.textFile("file:///E:/data/hello.txt")
//        val lines:RDD[String] = sc.textFile("hdfs://ns1/data/spark/streaming/test/hello.txt")
        println("partition: " + lines.getNumPartitions)
//        lines.foreach(line => println(line))
        val words:RDD[String] = lines.flatMap(line => line.split("\\s+"))
//        words.foreach(word => println(word))
        val pairs:RDD[(String, Int)] = words.map(word => (word, 1))
//      pairs.foreach{case (word, count) => println(word + "--->" + count)}
        //select key, count(*) from t group by key
        val ret:RDD[(String, Int)] = pairs.reduceByKey((v1, v2) => {
//            val i = 1 / 0
            v1 + v2
        })

        ret.foreach{case (word, count) => println(word + "--->" + count)}
        sc.stop()
    }
}
