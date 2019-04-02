package com.desheng.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object _05SparkGroupTopNOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("SparkGroupTopNOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        val lines = sc.textFile("file:///E:/data/spark/topn.txt")

        val course2Info:RDD[(String, String)] = lines.map(line => {
            val fields = line.split("\\s+")
            (fields(0), s"${fields(1)}_${fields(2)}")
        })
//        groupTopN(sc, course2Info)
        groupTopN2(sc, course2Info)
//        groupTopN3(sc, course2Info)
        sc.stop()
    }

    /**
      *
      * 真正的优化
      *     有10个文件，每个文件有100个数字，要求计算出这10个文件中最大的10个数。
      *     1、集中式的计算：最简单的如果资源能够允许，把这10个文件中的所有的数据加载到一个数组中进行排序，求前10
      *     2、分布式的思想：求出每个文件中的最大的10个数字，10个文件最后又100个数字，最后再对这100个数字进行排序求前十
      * @param sc
      * @param course2Info
      */
    def groupTopN3(sc:SparkContext, course2Info:RDD[(String, String)]): Unit = {
        def createCombiner(info:String):mutable.TreeSet[String] = {
            val ts = new mutable.TreeSet[String]()(new Ordering[String](){
                override def compare(x: String, y: String) = {
                    val xScore = x.substring(x.indexOf("_") + 1).toInt
                    val yScore = y.substring(y.indexOf("_") + 1).toInt
                    yScore.compareTo(xScore)
                }
            })
            ts.add(info)
            ts
        }

        def mergeValue(ts:mutable.TreeSet[String], info:String):mutable.TreeSet[String] = {
            ts.add(info)
            if(ts.size > 3) {
                ts.dropRight(ts.size - 3)
            } else {
                ts
            }
        }

        def mergeCombiners(ts1:mutable.TreeSet[String], ts2:mutable.TreeSet[String]):mutable.TreeSet[String] = {
            val ts = new mutable.TreeSet[String]()(new Ordering[String](){
                override def compare(x: String, y: String) = {
                    val xScore = x.substring(x.indexOf("_") + 1).toInt
                    val yScore = y.substring(y.indexOf("_") + 1).toInt
                    yScore.compareTo(xScore)
                }
            })
            for(info <- ts1.++:(ts2)) {
                ts.add(info)
            }
            if(ts.size > 3) {
                ts.dropRight(ts.size - 3)
            } else {
                ts
            }
        }

        val gbkRDD = course2Info.combineByKey(createCombiner, mergeValue, mergeCombiners)
        gbkRDD.foreach(println)
    }

    /**
      * 因为groupByKey的效率问题，所以我们使用combineByKey去实现
      * @param sc
      * @param course2Info
      */
    def groupTopN2(sc:SparkContext, course2Info:RDD[(String, String)]): Unit = {
        def createCombiner(info:String):mutable.TreeSet[String] = {
            val ts = new mutable.TreeSet[String]()
            ts.add(info)
            ts
        }

        def mergeValue(ts:mutable.TreeSet[String], info:String):mutable.TreeSet[String] = {
            ts.add(info)
            ts
        }
        def mergeCombiners(ts1:mutable.TreeSet[String], ts2:mutable.TreeSet[String]):mutable.TreeSet[String] = {
            ts1.++:(ts2)
        }
        val gbkRDD = course2Info.combineByKey(createCombiner, mergeValue, mergeCombiners)
        gbkRDD.foreach(println)
        println("开始排序=================>")
        val topN = 3
        //求组内的TopN--Top3
        val sortedRDD:RDD[(String, mutable.TreeSet[String])] = gbkRDD.map{case (course, infos) => {
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
            (course, ts)
        }}

        sortedRDD.foreach{case (course, top3) => {
            println(course + "--->" + top3)
        }}
    }
    /**
      * 分组topN，是对topN的扩展
      */
    def groupTopN(sc:SparkContext, course2Info:RDD[(String, String)]): Unit = {
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
            (course, ts)
        }}

        sortedRDD.foreach{case (course, top3) => {
            println(course + "--->" + top3)
        }}
    }
}
