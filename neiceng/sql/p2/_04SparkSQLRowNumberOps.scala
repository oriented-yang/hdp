package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * row_number来完成分组TopN的统计
  *     求取每个科目中成绩前三的同学
  */
object _04SparkSQLRowNumberOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLRowNumber")
            .master("local[*]")
            .getOrCreate()
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
              |  score,
              |  row_number() over(partition by course order by score desc) rank
              |from stu
              |having rank < 4
            """.stripMargin
        spark.sql(sql).show()

        spark.stop()
    }
}

case class Student(course:String, name:String, score:Int)