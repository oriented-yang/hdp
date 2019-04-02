package com.desheng.bigdata.spark.scala.core.p2

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * 常见的Action操作
  */
object _03ActionOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("ActionOps")
            .setMaster("local[4]")
        val sc = new SparkContext(conf)

        val list = List(
            "guo qi qi",
            "fu bo jie jie",
            "old li li",
            "li lv li"
        )
        val listRDD = sc.parallelize(list)
        val pairsRDD: RDD[(String, Int)] = listRDD
            .flatMap(_.split("\\s+"))
            .map(t => (t, 1))
        println("pairsRDD's partition: " + pairsRDD.getNumPartitions)
        println("----------reduce-----------")
        /*
            1)reduce：返回值的类型和原来的类型一致
            li-lv-li-lv, 4
         */
        val t:(String, Int) = pairsRDD.reduce((t1:(String, Int), t2:(String, Int)) => {
            (t1._1 + "_" + t2._1, t1._2 + t2._2)
        })
        println(t)
        println("----------collect-----------")
        /*
            collect：将rdd的数据拉取到driver中，容易造成driver的OOM，所以慎用
         */
        val colArr:Array[(String, Int)] = pairsRDD.collect()
        println(colArr.mkString("[", ",", "]"))
        println("----------count-----------")
        /**
          * count 获取rdd中有多少条记录
          */
        println("pairsRDD's size: " + pairsRDD.count())
        println("----------take(n)-----------")
        /**
          * take(n)，获取rdd的前N条记录
          * 作业：在sortByKey的基础之上，求取Top3，或者不用sortByKey也可以
          * takeOrdered()
          */
        val takes:Array[(String, Int)] = pairsRDD.take(3)
        println(takes.mkString("[", ",", "]"))
        //        5)first: --> 就是take(1)
        println("----------saveAsTextFile-----------")
        /*
            6)saveAsTextFile：讲结果保存成为普通的文本文件
            如果目录存在，会报错 FileAlreadyExistsException
            saveAsObjectFile:将结果以SequenceFile的方式写到hdfs
            saveAsHadoopFile中使用OutputFormat是org.apache.hadoop.mapred.OutputFormat
            saveAsNewAPIHadoopFile中使用OutputFormat是org.apache.hadoop.mapreduce.OutputFormat
         */
//        pairsRDD.saveAsTextFile("file:///E:/data/out/1810-bd")
//        pairsRDD.saveAsTextFile("hdfs://ns1/output/1810-bd")
//        pairsRDD.saveAsObjectFile("file:///E:/data/out/1810-bd")
          pairsRDD.saveAsHadoopFile(
              "file:///E:/data/out/1810-bd",
              classOf[Text],
              classOf[IntWritable],
              classOf[TextOutputFormat[Text, IntWritable]]
          )
//          pairsRDD.saveAsNewAPIHadoopFile()
            /*
                countByKey：和reduceByKey效果相同，但reduceByKey是一个Transformation
                    计算每个key出现多少次，
             */
        println("----------countByKey-----------")
        val map:scala.collection.Map[scala.Predef.String, Long] = pairsRDD.countByKey()
        map.foreach(println)
        sc.stop()
    }
}
