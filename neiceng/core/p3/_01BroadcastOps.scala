package com.desheng.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量的使用
  */
object _01BroadcastOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("BroadcastOp")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        val provinces = List(
            "1,陕西",
            "2,甘肃",
            "3,广东",
            "4,河北",
            "5,山东"
        ).map(t => {
            val fields = t.split(",")
            (fields(0).toInt, fields(1))
        }).toMap
        //--->广播变量
        val provinceMap:Broadcast[Map[Int, String]] = sc.broadcast(provinces)
        val stuList = List(
            "1,曹凯路,1",
            "2,吕萌,2",
            "3,胡俊杰,3",
            "4,杨洋,1",
            "5,刘世昌,6"
        )

        val stuRDD:RDD[String] = sc.parallelize(stuList)


        stuRDD.foreach(info => {
            val fields = info.split(",")
            val sid = fields(0).toInt
            val name = fields(1)
            val pid = fields(2).toInt
            val province = provinceMap.value.getOrElse(pid, "北京")
            println(s"${sid}\t${name}\t${province}")
        })
        sc.stop()
    }

    /**
      * join：表的关联
      *     左表A：[K, V]
      *     右表B：[K, W]
      *     交叉连接：across join --->没有连接条件on --->笛卡尔积
      *     内连接  : inner join --->等值连接：返回左右两张表的交集
      *         A.join(B)--->[K, (V, W)]
      *     外连接  ： outer join --->不等值连接
      *        左外连接：左表A left outer join 右表B，返回左表A的所有数据，右表B向的匹配数据，匹配不了返回null
      *         A.leftOuterJoin(B)--->[K, (V, Option[W])]
      *        右外连接：左表A right outer join 右表B，返回右表B的所有数据，左表A的匹配数据，匹配不了返回null
      *         A.rightOuterJoin(B)--->[K, (Option[V], W)]
      *        全连接：左表A full outer join 右表B <==> 左外连接+右外连接
      *         A.fullOuterJoin(B)--->[K, (Option[V], Option[W])]
      *
      * 学生表stu
      *     id name pid
      * 地区
      *     pid  province
      */
    def joinOps(sc: SparkContext): Unit = {
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
