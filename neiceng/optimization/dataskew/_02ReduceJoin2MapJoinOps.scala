package com.desheng.bigdata.spark.scala.optimization.dataskew

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ReduceJoin ---> MapJoin
  */
object _02ReduceJoin2MapJoinOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("ShuffleOptimization")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
//        reduceJoinOps(sc)
        mapJoinOps(sc)
        sc.stop()
    }
    /*
        大小表关联，将小表广播
        select


        from student s
        left join province p on s.pid = p.pid
     */
    def  mapJoinOps(sc: SparkContext): Unit = {
        val provinces = List(
            "1,陕西",
            "2,甘肃",
            "3,广东",
            "4,河北",
            "5,山东",
            "6,河南",
            "7,山西"
        ).map(line => {
            val fields = line.split(",")
            val pid = fields(0).toInt
            val province = fields(1)
            (pid, province)
        }).toMap
        val pBC:Broadcast[Map[Int, String]] = sc.broadcast(provinces)
        val stuList = List(
            "1,曹凯路,1",
            "2,吕萌,2",
            "3,胡俊杰,3",
            "4,杨洋,1",
            "5,刘世昌,6"
        )
        val stuRDD:RDD[String] = sc.parallelize(stuList)

        //学生
        val sid2Info:RDD[(String, (Int, String))] = stuRDD.map(line => {
            val fields = line.split(",")
            val sid = fields(0).toInt
            val name = fields(1)
            val pid = fields(2).toInt
            val province = pBC.value.getOrElse(pid, "北京")
            (province, (sid, name))
        })
        sid2Info.foreach(println)
    }

    /**
      * join：表的关联
      * 学生表stu
      *     id name pid
      * 地区--->字典表
      *     pid  province
      */
    def reduceJoinOps(sc: SparkContext): Unit = {
        val provinceList = List(
            "1,陕西",
            "2,甘肃",
            "3,广东",
            "4,河北",
            "5,山东"
        )
        val stuList = List(
            "1,曹凯路,1",
            "2,吕萌,2",
            "3,胡俊杰,3",
            "4,杨洋,1",
            "5,刘世昌,6"
        )
        val provinceRDD:RDD[String] = sc.parallelize(provinceList)
        val stuRDD:RDD[String] = sc.parallelize(stuList)
        //省份
        val pid2ProvinceRDD:RDD[(Int, String)] = provinceRDD.map(line => {
            val fields = line.split(",")
            val pid = fields(0).toInt
            val province = fields(1)
            (pid, province)
        })
        //学生
        val sid2Info:RDD[(Int, (Int, String))] = stuRDD.map(line => {
            val fields = line.split(",")
            val sid = fields(0).toInt
            val name = fields(1)
            val pid = fields(2).toInt
            (pid, (sid, name))
        })
        //学生的所有信息
        val stuJoinInfo:RDD[(Int, ((Int, String), String))] = sid2Info.join(pid2ProvinceRDD)
        println("----------内连接之后的数据--------------")
        stuJoinInfo.foreach{case (pid, ((sid, name), province)) => {
            println(s"pid:${pid}\tsid:${sid}\tname:${name}\tprovince:${province}")
        }}

    }
}
