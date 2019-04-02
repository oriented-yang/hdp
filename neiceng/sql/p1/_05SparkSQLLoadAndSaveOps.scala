package com.desheng.bigdata.spark.scala.sql.p1

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * SparkSQL数据的加载和落地
  */
object _05SparkSQLLoadAndSaveOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("SparkSQLLoadAndSave")
            .getOrCreate()
//        readOps(spark)
        writeOps(spark)
        spark.stop()
    }
    def writeOps(spark:SparkSession): Unit = {
        val stuDF = spark.read.csv("file:///E:/data/spark/sql/student.csv")
            //toDF用户重构DataFrame的列名
            .toDF("id", "name", "age", "gender", "course", "score")
        stuDF.show()


        /**
          * 保存的时候可以指定SaveMode
          *     ErrorIfExists：目录存在则报错
          *     Append      ：追加
          *     Overwrite   ：覆盖（删除+重建）
          *     Ignore      ：忽略(有则忽略，无则执行)
          */
//        stuDF.write.mode(SaveMode.Append).format("json").save("file:///E:/data/spark/sql/stu.csv")
//        stuDF.write.mode(SaveMode.Overwrite).orc("file:///E:/data/spark/sql/stu.csv")
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "stu"
        val properties = new Properties()
        properties.put("user", "root")
        properties.put("password", "sorry")
        stuDF.write.mode(SaveMode.Append).jdbc(url, table, properties)
    }
    def readOps(spark:SparkSession): Unit = {
        //使用load加载
        var pDF = spark.read.load("file:///E:/data/spark/sql/users.parquet")
        pDF.show()
        println("=======json文件====")
        pDF = spark.read.format("json").load("file:///E:/data/spark/sql/people.json")
        pDF.show()
        println("===========常见的加载数据的方式=============")
        import  spark.implicits._
        val csDS = spark.read.text("file:///E:/data/spark/sql/topn.txt").map(row => {
            val line = row.getString(0)
            val fields = line.split("\\s+")
            ClassScore(fields(0), fields(1).toInt)
        })
        csDS.show()
        println("---------orc-----------")
        spark.read.orc("file:///E:/data/spark/sql/student.orc").show()
        println("---------csv-----------")
        val stuDF = spark.read.csv("file:///E:/data/spark/sql/student.csv")
            //toDF用户重构DataFrame的列名
            .toDF("id", "name", "age", "gender", "course", "score")
        stuDF.show()
        println("---------jdbc-----------")
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "lagou"
        val properties = new Properties()
        properties.put("user", "root")
        properties.put("password", "sorry")
        val jdbcDF = spark.read.jdbc(url, table, properties)
        jdbcDF.show()
        //简单统计，不同教育背景的招聘java规模
        jdbcDF.createOrReplaceTempView("job")
        val sql =
            """
              |select
              | t_edu,
              | count(1) as countz
              |from job
              |where instr(lower(t_job), 'java') > 0
              |group by t_edu
            """.stripMargin
        spark.sql(sql).show()
    }
}

case class ClassScore(clazz:String, score:Int)