package com.desheng.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _02AccumulatorOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("AccumulatorOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        mapOps(sc.parallelize(1 to 10))

        sc.stop()
    }

    /**
      * 统计rdd中偶数的个数，不用filter的方式
      * @param listRDD
      */
    def mapOps(listRDD:RDD[Int]): Unit = {
        println("原集合元素：")
        var count = 0
        listRDD.collect.foreach(tt => print(tt + "\t"))
        val retRDD:RDD[Int] = listRDD.map(num => {
            if(num % 2 == 0)
                count += 1
            num * 7
        })
        println("\r\n变换之后的元素：")
        println("偶数有：" + count)
        retRDD.foreach(t => print(t + "\t"))
    }
}
