package com.desheng.bigdata.spark.scala.optimization.dataskew

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据倾斜解决方案之过滤掉少数的几个key
  * //假设上述在计算wordcount的时候发生数据倾斜
  * 第一步：
  *     通过sample算子找到发生数据倾斜的那（几）个key
  * 第二步：
  *     执行各种不同的方案操作
  *     本例，就是讲这（几）个key进行过滤
  */
object _01SparkDataSkewFilterOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("DataSkewFilter")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        filterDS(sc)
        sc.stop()
    }
    def filterDS(sc: SparkContext) = {
        val list = List(
            "hello peng peng peng peng peng tan",
            "tan li qianr qianr qianr peng qianr",
            "peng li peng"
        )
        val listRDD = sc.parallelize(list)
        val words = listRDD.flatMap(line => line.split("\\s+"))

        val pairs = words.map((_, 1))
        //sample抽样
        println("===========sample的数据===============")
        val smpaledRDD = pairs.sample(true, 0.6)
        val sampledRBK = smpaledRDD.reduceByKey(_+_)
        sampledRBK.foreach(println)
        println("===========sample Top1===============")
        val dataskewKeys:Array[String] = sampledRBK.takeOrdered(1)(new Ordering[(String, Int)](){
            override def compare(x: (String, Int), y: (String, Int)) = {
                y._2.compareTo(x._2)
            }
        }).map(_._1)
        dataskewKeys.foreach(println)
        println("===========执行过滤操作===============")
        pairs.filter{case (key, count) => {
            !dataskewKeys.contains(key)
        }}.reduceByKey(_+_, 5).foreach(println)
    }
}
