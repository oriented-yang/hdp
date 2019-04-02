package com.desheng.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

//spark的排序操作
object _04SparkOrderOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("SparkOrder")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)

//        topN(sc)
//        secondarySort(sc)
        groupTopN(sc)
        sc.stop()
    }

    /**
      * 分组topN，是对topN的扩展
      * 数据格式：
      * 科目\t姓名\t成绩
      * 需求：求出各科目成绩前3的同学
      * @param sc
      */
    def groupTopN(sc:SparkContext): Unit = {
        val lines = sc.textFile("file:///E:/data/spark/topn.txt")

        val course2Info:RDD[(String, String)] = lines.map(line => {
            val fields = line.split("\\s+")
            (fields(0), s"${fields(1)}_${fields(2)}")
        })
        course2Info.foreach(println)
        println("开始排序=================>")
        //分组
        val course2Infos:RDD[(String, Iterable[String])] = course2Info.groupByKey()
        val topN = 3
        //求组内的TopN--Top3
        val sortedRDD:RDD[(String, mutable.TreeSet[String])] = course2Infos.map{case (course, infos) => {
            //infos便是该course对应的所有的学生成绩
            var ts = new mutable.TreeSet[String]()(new Ordering[String](){
                override def compare(x: String, y: String) = {
                    val xScore = x.substring(x.indexOf("_") + 1).toInt
                    val yScore = y.substring(y.indexOf("_") + 1).toInt
                    yScore.compareTo(xScore)
                }
            })
            for (info <- infos) {
                ts.add(info)
                if(ts.size > topN) {
                    ts = ts.dropRight(ts.size - topN)
                }
            }
            //
            (course, ts)
        }}

        sortedRDD.foreach{case (course, top3) => {
            println(course + "--->" + top3)
        }}
    }
    /**
      * 二次排序
      * 20 21
        50 51
        50 52
        50 53
        50 54
        60 51
        60 53
        按照第一列的升序排序，如果第一列相同按照第二列的降序排序
        要想进行排序的话，sortByKey
        此时我们的数据有两列内容，所以让谁作为key都不完整
        所以，为了完成二次排序，只能让这两列都作为key
      * @param sc
      */
    def secondarySort(sc:SparkContext): Unit = {
        val lines = sc.textFile("file:///E:/data/spark/secondsort.csv")
        //将这两列封装到一个对象
        val pairs:RDD[(SecondSort, Int)] = lines.map(line => {
            val fields = line.split("\\s+")
            (new SecondSort(fields(0).toInt, fields(1).toInt), 1)
        })
//        pairs.foreach(t => println(t._1))
        val sorted = pairs.sortByKey(ascending = true, numPartitions = 1)
        sorted.foreach(t => {
            println(t._1)
        })
        //第二种方式使用sortBy
        println("=============sortBy==================")
        lines.sortBy(
            line => {
                val fields = line.split("\\s+")
                (fields(0).toInt, fields(1).toInt)
            },
            ascending = true,
            numPartitions = 1)(
            new Ordering[(Int, Int)]() {
                override def compare(x: (Int, Int), y: (Int, Int)) = {
                    var ret = x._1.compareTo(y._1)
                    if(ret == 0) {
                        ret = y._2.compareTo(x._2)
                    }
                    ret
                }
            },
            ClassTag.Object.asInstanceOf[ClassTag[(Int, Int)]]
        ).foreach(println)
    }

    def topN(sc:SparkContext): Unit = {
        val lines = sc.textFile("file:///E:/data/hello.txt")
        val rbkRDD = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)

        //求单词数最高的前3位Top3
        val sortedRDD:RDD[(String, Int)] = rbkRDD
            .map(t => (t._2, t._1))//按照value进行排序--->k-v的转换
            .sortByKey(ascending = false, numPartitions = 1)
            .map(t => (t._2, t._1))//排序完毕之后进行还原

        sortedRDD.take(3).foreach(println)
        println("============takeOrder==========")
        rbkRDD.takeOrdered(3)(new Ordering[(String, Int)](){
            override def compare(x: (String, Int), y: (String, Int)) = {
                y._2.compareTo(x._2)
            }
        }).foreach(println)
    }
}
class SecondSort extends Ordered[SecondSort] with Serializable {
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