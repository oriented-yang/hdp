package com.desheng.bigdata.spark.scala.core.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object _01SparkScalaWordCount {
    def main(args: Array[String]): Unit = {
        if(args == null || args.length < 1) {
            println(
                """Parameter Errors! Usage: <inputpath>
                  |inputpath:   输入文件源地址
                """.stripMargin)
            System.exit(-1)
        }
//        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//        Logger.getLogger("org.spark-project").setLevel(Level.WARN)

        val Array(inputpath) = args//模式匹配

        val conf = new SparkConf()
        //master
        conf.setAppName(s"${_01SparkScalaWordCount.getClass.getSimpleName}")
        val sc = new SparkContext(conf)
        //加载本地的文件
        val lines:RDD[String] = sc.textFile(inputpath)
        println("partition: " + lines.getNumPartitions)
        val words:RDD[String] = lines.flatMap(line => line.split("\\s+"))
        val pairs:RDD[(String, Int)] = words.map(word => (word, 1))
        val ret:RDD[(String, Int)] = pairs.reduceByKey((v1, v2) => {
            v1 + v2
        })
        ret.collect//collect操作将集群中的数据拉取到driver本地
            .foreach{case (word, count) => println(word + "--->" + count)}
        sc.stop()
    }
}
