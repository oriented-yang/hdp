package com.desheng.bigdata.spark.scala.core.p3

import com.desheng.bigdata.spark.scala.core.p3.accumulator.UserDefineAccumulator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

object _03AccumulatorOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("_03AccumulatorOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
//        mapOps(sc.parallelize(1 to 10))
//        accuOps(sc)
        accuOps1(sc)

        Thread.sleep(100000000)
        sc.stop()
    }
    def accuOps1(sc: SparkContext): Unit = {
        val lines = sc.textFile("file:///E:/data/spark/core/10w行数据.txt")

        val udAccu = new UserDefineAccumulator
        sc.register(udAccu, "userDefinedAcc")

        val pairRDD:RDD[(String, Int)] = lines.flatMap(_.split("\\s+")).map(word => {
            if(word == "handle")
                udAccu.add(word)
            if(word == "is")
                udAccu.add(word)
            if(word == "scale")
                udAccu.add(word)
            (word, 1)
        })
        val rbkRDD = pairRDD.reduceByKey(_+_)
        rbkRDD.count()
        println(udAccu.value)

    }

    /**
      * 统计文件中出现的handle有多少次
      *     1、使用累加器可以减少spark作业的提交
      *     2、累加器重复调用会造成数据的重复累加
      * @param sc
      */
    def accuOps(sc: SparkContext): Unit = {
        val lines = sc.textFile("file:///E:/data/spark/core/10w行数据.txt")
        //使用filter过滤的方式来完成
//        val rbkRDD:RDD[(String, Int)] = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
//        rbkRDD.foreach(println)
//        rbkRDD.filter(_._1 == "handle").foreach(println)
        //使用累加器
        val accu = sc.longAccumulator("wordAccu")
        val isAccu = sc.longAccumulator("isAccu")
        val scaleAccu = sc.longAccumulator("scaleAccu")
        val pairRDD:RDD[(String, Int)] = lines.flatMap(_.split("\\s+")).map(word => {
            if(word == "handle")
                accu.add(1L)
            if(word == "is")
                isAccu.add(1L)
            if(word == "scale")
                scaleAccu.add(1L)
            (word, 1)
        })
        val rbkRDD = pairRDD.reduceByKey(_+_)
        rbkRDD.foreach(println)
        println("累加handle的次数：" + accu.value)//20000
        accu.reset()//累加器重置
        pairRDD.filter(_._1 == "handle").foreach(println)
        println("累加handle的次数：" + accu.value)//40000
    }
    /**
      * 统计rdd中偶数的个数，不用filter的方式
      * @param listRDD
      */
    def mapOps(listRDD:RDD[Int]): Unit = {
        println("原集合元素：")
        //声明累加器
        val evenAccu:LongAccumulator = listRDD.sparkContext.longAccumulator("evenAccu")

        listRDD.collect.foreach(tt => print(tt + "\t"))
        val retRDD:RDD[Int] = listRDD.map(num => {
            if(num % 2 == 0)
                evenAccu.add(1L)
            num * 7
        })
        println("\r\n变换之后的元素：")
        retRDD.foreach(t => print(t + "\t"))
        //累加器的调用，必须要在action之后
        println("偶数有：" + evenAccu.value)
    }
}
