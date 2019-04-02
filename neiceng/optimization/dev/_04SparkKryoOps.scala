package com.desheng.bigdata.spark.scala.optimization.dev

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

//spark的排序操作
object _04SparkKryoOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("SparkKryoOps")
            .setMaster("local[*]")
            //第一步使用kryo
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .registerKryoClasses(Array(classOf[SecondSort]))
        val sc = new SparkContext(conf)

        secondarySort(sc)
        sc.stop()
    }
    def secondarySort(sc:SparkContext): Unit = {
        val lines = sc.textFile("file:///E:/data/spark/secondsort.csv")
        //将这两列封装到一个对象
        val pairs:RDD[(SecondSort, Int)] = lines.map(line => {
            val fields = line.split("\\s+")
            (new SecondSort(fields(0).toInt, fields(1).toInt), 1)
        })
        val sorted = pairs.sortByKey(ascending = true, numPartitions = 1)
        sorted.foreach(t => {
            println(t._1)
        })
    }

}
class SecondSort extends Ordered[SecondSort] {
    private var first:Int = _
    private var second:Int = _

    def this(first:Int, second:Int) {
        this()
        this.first = first
        this.second = second
    }
    override def compare(that: SecondSort): Int = {
        var ret = this.first.compareTo(that.first)
        if(ret == 0) {
            ret = that.second.compareTo(this.second)
        }
        ret
    }
    override def toString: String = this.first + "\t" + this.second
}