package com.desheng.bigdata.spark.scala.core.p2

import com.desheng.bigdata.spark.scala.core.p1._01SparkScalaWordCount
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * 通过持久化和未经持久化进行一下比较
  */
object _04SparkPersistOps {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        //master
        conf.setAppName(s"${_04SparkPersistOps.getClass.getSimpleName}")
        conf.setMaster("local[*]")
        val sc = new SparkContext(conf)
        val start = System.currentTimeMillis()
        //加载本地的文件
        val lines:RDD[String] = sc.textFile("file:///E:/data/spark/core/sequences.txt")
        var count = lines.count()
        println("未持久化===》linesRDD's count: " + count + ", cost time: " + (System.currentTimeMillis() - start) + "ms")

//        lines.persist()
//        lines.cache()
        lines.persist(StorageLevel.MEMORY_ONLY)
        val start1 = System.currentTimeMillis()
        val count1 = lines.count()
        println("持久化===》linesRDD's count: " + count1 + ", cost time: " + (System.currentTimeMillis() - start1) + "ms")
        sc.stop()
    }
}
