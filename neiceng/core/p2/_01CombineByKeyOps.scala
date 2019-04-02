package com.desheng.bigdata.spark.scala.core.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 使用combineByKey模拟groupByKey和reduceByKey
  */
object _01CombineByKeyOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("CombineByKeyOps")
            .setMaster("local[4]")
        val sc = new SparkContext(conf)

        val list = List(
            "guo qi qi",
            "fu bo jie jie",
            "old li li",
            "li lv li"
        )

//        cbk2rbk(sc, list)
//        cbk2gbk(sc, list)
        cbk2gbk1(sc, list)

        sc.stop()
    }

    def cbk2gbk1(sc: SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        val pairsRDD:RDD[(String, String)] = listRDD
            .flatMap(_.split("\\s+"))
            .map(t => (t, t))
        println("pairsRDD's partition: " + pairsRDD.getNumPartitions)

        println("-----------使用groupByKey的结果----------------")
        var retRDD:RDD[(String, Iterable[String])] = pairsRDD.groupByKey()
        retRDD.foreach(println)
        println("-----------使用combineByKey的结果----------------")

        def createCombiner(word:String):ArrayBuffer[String] = {
            println("----------createCombiner----------------")
            println("createCombiner--->" + word)
            val ab = ArrayBuffer[String]()
            ab.append(word)
            ab
        }

        def mergeValue(ab:ArrayBuffer[String], word:String):ArrayBuffer[String] = {
            println("----------mergeValue----------------")
            println("mergeValue==>" + ab + "----> " + word)
            ab.append(word)
            ab
        }
        def mergeCombiners(ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]):ArrayBuffer[String] = {
            println("----------mergeCombiners----------------")
            println("mergeCombiners==>" + ab1 + "----> " + ab2)
            ab1.++:(ab2)
        }

        val retRDD1:RDD[(String, ArrayBuffer[String])] = pairsRDD
            .combineByKey(createCombiner, mergeValue, mergeCombiners)

        retRDD1.foreach(println)
    }
    /**
      * combineByKey --->groupByKey
      */
    def cbk2gbk(sc: SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        val pairsRDD:RDD[(String, Int)] = listRDD
            .flatMap(_.split("\\s+"))
            .map((_, 1))
        println("-----------使用groupByKey的结果----------------")
        var retRDD:RDD[(String, Iterable[Int])] = pairsRDD.groupByKey()
        retRDD.foreach(println)
        println("-----------使用combineByKey的结果----------------")
        def createCombiner(num:Int):ArrayBuffer[Int] = {
            val ab = ArrayBuffer[Int]()
            ab.append(num)
            ab
        }
        def mergeValue(ab:ArrayBuffer[Int], num:Int):ArrayBuffer[Int] = {
            ab.append(num)
            ab
        }
        def mergeCombiners(ab1:ArrayBuffer[Int], ab2:ArrayBuffer[Int]):ArrayBuffer[Int] = {
            ab1.++:(ab2)
        }

        val retRDD1:RDD[(String, ArrayBuffer[Int])] = pairsRDD
            .combineByKey(createCombiner, mergeValue, mergeCombiners)

        retRDD1.foreach(println)
    }
    /**
      * combineByKey --->reduceByKey
      */
    def cbk2rbk(sc: SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        val pairsRDD:RDD[(String, Int)] = listRDD
                    .flatMap(_.split("\\s+"))
                    .map((_, 1))
        println("-----------使用reduceByKey的结果----------------")
        var retRDD:RDD[(String, Int)] = pairsRDD.reduceByKey(_+_)
        retRDD.foreach(println)
        println("-----------使用combineByKey的结果----------------")

        retRDD = pairsRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)

        retRDD.foreach(println)
    }

    /** 1+...+10
      * var sum = 1 --->createCombiner(num:Int)初始化
      * for(num <- 2 to 10) {
      *     sum += num --->mergeValue(sum:Int, num:Int) 合并
      * }
      * println(sum)
      *
      */
    //初始化
    def createCombiner(num:Int) : Int = {
        num
    }
    //分区内的合并
    def mergeValue(sum:Int, num:Int):Int = {
        sum + num
    }
    //分区间的合并
    def mergeCombiners(sum1:Int, sum2:Int) = {
        sum1 + sum2
    }
}
