package com.desheng.bigdata.spark.scala.core.p2

import com.desheng.bigdata.spark.scala.core.p2._01CombineByKeyOps.cbk2gbk1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object _02AggregateByKeyOps {


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

//        abk2rbk(sc, list)
        abk2gbk(sc, list)

        sc.stop()
    }
    /**
      * 使用aggregateByKey模拟groupByKey
      * @param sc
      * @param list
      */
    def abk2gbk(sc: SparkContext, list: List[String]) = {
        val listRDD = sc.parallelize(list)
        val pairsRDD: RDD[(String, String)] = listRDD
            .flatMap(_.split("\\s+"))
            .map(t => (t, t))
        println("pairsRDD's partition: " + pairsRDD.getNumPartitions)

        println("-----------使用groupByKey的结果----------------")
        var retRDD: RDD[(String, Iterable[String])] = pairsRDD.groupByKey()
        retRDD.foreach(println)
        println("-----------使用aggregateByKey的结果----------------")

        val retRDD1:RDD[(String, ArrayBuffer[String])] = pairsRDD.aggregateByKey(ArrayBuffer[String]())(
            seqOp,
            combOp
        )
        retRDD1.foreach(println)
    }

    //分区内的聚合
    def seqOp(ab:ArrayBuffer[String], word:String): ArrayBuffer[String] = {
        println("seqOp======>" + ab + "---->" + word)
        ab.append(word)
        ab
    }
    //分区间的聚合
    def combOp(ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]): ArrayBuffer[String] = {
        println("combOp======>" + ab1 + "---->" + ab2)
        ab1.++:(ab2)
    }
    /**
      * 使用aggregateByKey模拟reduceByKey
      * @param sc
      * @param list
      */
    def abk2rbk(sc: SparkContext, list: List[String]) = {
        val listRDD = sc.parallelize(list)
        val pairsRDD:RDD[(String, Int)] = listRDD
            .flatMap(_.split("\\s+"))
            .map(t => (t, 1))
        println("pairsRDD's partition: " + pairsRDD.getNumPartitions)

        println("-----------使用reduceByKey的结果----------------")
        var retRDD:RDD[(String, Int)] = pairsRDD.reduceByKey(_+_)
        retRDD.foreach(println)
        println("-----------使用aggregateByKey的结果----------------")

        /** 1+...+10
          * var sum = 0 --->zeroValue初始化
          * for(num <- 1 to 10) {
          *     sum += num --->seqOp 分区内合并
          * }
          * println(sum)
          *
          */
        retRDD = pairsRDD.aggregateByKey(0)(
            (mergeVal: Int, value: Int) => mergeVal + value,
            (partMergeVal1: Int, partMergeVal2: Int) => partMergeVal1 + partMergeVal2
        )
        retRDD.foreach(println)
    }
}
