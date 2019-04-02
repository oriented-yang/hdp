package com.desheng.bigdata.spark.scala.optimization.dataskew

import java.util.Random

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

/**
  * 采样倾斜的key并分拆进行join
  */
object _03SplitJoinOps{
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("ShuffleOptimization")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        joinOps(sc)
        sc.stop()
    }
    /*

     */
    def  joinOps(sc: SparkContext): Unit = {
        val list1 = List(
            "hello you hello you",
            "hello hello hello me"
        )

        val list2 = List(
            ("hello", "handsome-body"),
            ("you", "shit"),
            ("me", "gay")
        )
        //<String, Int>
        val rdd1 = sc.parallelize(list1).flatMap(_.split("\\s+")).map((_, 1))
        //<String, String>
        val rdd2 = sc.parallelize(list2)
        //rdd1.join(rdd2)
        /*
            step1、从rdd1中抽样找到有数据倾斜的key
            val dataskews = rdd1.sample().reduce.so...
         */
        println("step1、从rdd1中抽样找到有数据倾斜的key")
        val dataskews:Array[String] = smaple(rdd1)//注意：如果dataskews中的数据较多，建议通过广播变量


        /*
            step2、根据dataskews将原先rdd1和rdd2分别进行拆分
            val dataskewRDD1 = rdd1.filter(key => dataskews.contains(key))
            val commonRDD1 = rdd1.filter(key => !dataskews.contains(key))

            val dataskewRDD2 = rdd2.filter(key => dataskews.contains(key))
            val commonRDD2 = rdd2.filter(key => !dataskews.contains(key))
         */
        println("step2、根据dataskews将原先rdd1和rdd2分别进行拆分")
        val dataskewRDD1 = rdd1.filter{case (key, value) => dataskews.contains(key)}
        val commonRDD1 = rdd1.filter{case (key, value) => !dataskews.contains(key)}

        val dataskewRDD2 = rdd2.filter{case (key, value) => dataskews.contains(key)}
        val commonRDD2 = rdd2.filter{case (key, value) => !dataskews.contains(key)}
        println("有数据倾斜的RDD：")
        dataskewRDD2.foreach(println)
        println("正常的RDD：")
        commonRDD2.foreach(println)
        /**
          * step3、对上述的dataskewRDD1进行随机打散
          * val prefixRDD1 = dataskewRDD1.map{case (key, v) => {
            val random = new Random()
            val prefix = random.nextInt(N)

            (prefix + "_" + key, v)
        }}
          */
        println("step3、对上述的dataskewRDD1进行随机打散")
        val prefixRDD1 = dataskewRDD1.map{case (key, v) => {
            val random = new Random()
            val prefix = random.nextInt(3)
            (prefix + "_" + key, v)
        }}
        prefixRDD1.foreach(println)
        /**
          * step4、对上述的dataskewRDD2进行扩容（依据是step3中的N）
          * val prefixRDD2 = dataskewRDD2.flatMap{case (key, v) => {
            val ab = ArrayBuffer[(K, V)](

            for(i <- 0 until N) {
                ab.append((i + "_" + key, v))
            }
        }}
          */
        println("step4、对上述的dataskewRDD2进行扩容（依据是step3中的N）")
        val prefixRDD2 = dataskewRDD2.flatMap{case (key, value) => {
            val ab = ArrayBuffer[(String, String)]()
            for(i <- 0 until 3) {
                ab.append((i + "_" + key, value))
            }
            ab
        }}
        println("扩容之后的RDD数据：")
        prefixRDD2.foreach(println)
        /**
          * step5、分别进行join操作
          * val commonRDD = commonRDD1.join(commonRDD2)
            val dataskewRDD = prefixRDD1.join(prefixRDD2)
            //去掉dataskewRDD的前缀
          */
        println("step5、分别进行join操作")
        val commonRDD = commonRDD1.join(commonRDD2)
        val prefixDataskewRDD = prefixRDD1.join(prefixRDD2)
        println("正常commonRDD：")
        commonRDD.foreach(println)
        println("倾斜rdd：")
        prefixDataskewRDD.foreach(println)
        println("去掉前缀结果：")
        val dataskewRDD = prefixDataskewRDD.map{case (key, (count, desc)) => {
            (key.substring(key.indexOf("_") + 1), (count, desc))
        }}
        dataskewRDD.foreach(println)
        /*
            step6、两个rdd进行union操作得到最后的结果
         */
        println("step6、两个rdd进行union操作得到最后的结果")
        val joinedRDD = commonRDD.union(dataskewRDD)
        joinedRDD.foreach(println)
        sc.stop()
    }


    def smaple(rdd:RDD[(String, Int)]): Array[String] = {
        println("===========sample的数据===============")
        val smpaledRDD = rdd.sample(true, 0.8)
        println("采样数据：")
        smpaledRDD.foreach(println)
        val samples = smpaledRDD.reduceByKey(_+_)
                .takeOrdered(1)(new Ordering[(String, Int)](){
                    override def compare(x: (String, Int), y: (String, Int)) = {
                        y._2.compareTo(x._2)
                    }
                })
        println("数据倾斜的key：" + samples.mkString(","))
        samples.map(_._1)
    }
}
