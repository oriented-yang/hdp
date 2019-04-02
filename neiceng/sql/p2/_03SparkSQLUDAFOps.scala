package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * UDAF
  *   步骤：
  *     1、要么扩展类或者接口，然后复写其中的方法
  *     2、进行注册
  *     3、直接进行调用
  *  通过模拟avg的操作来学习udaf的构建
  */
object _03SparkSQLUDAFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLUDAFOps")
            .master("local[*]")
            .getOrCreate()
        //step2、注册
        spark.udf.register("myAvg", new MyAVGUDAF())
        import spark.implicits._
        val df = spark.read.text("file:///E:/data/spark/topn.txt").toDF("line")
        val stuDS = df.map(row => {
            val fields = row.getString(0).split("\\s+")
            val course = fields(0)
            val name = fields(1)
            val score = fields(2).toInt
            Student(course, name, score)
        }).as[Student]
        stuDS.createTempView("stu")
        val sql =
            """
              |select
              |  course,
              |  round(avg(score), 2) official_avg,
              |  round(myAvg(score), 2) my_avg
              |from stu
              |group by course
            """.stripMargin
        spark.sql(sql).show()

        spark.stop()
    }
}

case class Student(course:String, name:String, score:Int)