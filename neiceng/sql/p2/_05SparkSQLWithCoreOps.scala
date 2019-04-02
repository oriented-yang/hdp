package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * SparkSQL和SparkCore的整合案例
  * 数据：
  *     2018-11-13  tom	china	beijing	pc	web
  * 2018-11-13	tom	news	tianjing	pc	web
  * 字段解释：
  * 日期         user keyword area    device  type
  * 需求：
  *     统计每天用户检索关键字的top3
  *  注意：同一个用户在同一天对同一个关键字的多次检索，只能算一次
  *
  */
object _05SparkSQLWithCoreOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLWithCoreOps")
            .master("local[*]")
            .getOrCreate()
        val lines = spark.sparkContext.textFile("file:///E:/data/spark/sql/dailykey.txt")

        val distinct:RDD[String] = lines.map(line => {
            val fields = line.split("\\s+")
            val date = fields(0)
            val user = fields(1)
            val keyword = fields(2)
            date + "_" + user + "_" + keyword
        }).distinct()

        val rowRDD:RDD[Row] = distinct.map(line => {
            val fields = line.split("_")
            val date = fields(0)
            val user = fields(1)
            val keyword = fields(2)
            Row(date, user, keyword)
        })
        val schema = StructType(List(
            StructField("date", DataTypes.StringType, false),
            StructField("user", DataTypes.StringType, false),
            StructField("keyword", DataTypes.StringType, false)
        ))
        val searchDF = spark.createDataFrame(rowRDD, schema)

        searchDF.createTempView("tmp")
        println("=============每一天每个关键的检索次数==========")
        var sql =
            """
              |select
              | date,
              | keyword,
              | count(1)
              |from tmp
              |group by date, keyword
            """.stripMargin
        spark.sql(sql).show()
        println("=============每一天关键的检索次数Top3==========")
        sql =
            """
              |select
              | t.date,
              | t.keyword,
              | t.countz,
              | row_number() over(partition by t.date order by t.countz desc) rank
              |from (
              | select
              |     date,
              |     keyword,
              |     count(1) countz
              | from tmp
              | group by date, keyword
              |) t
              |having rank < 4
            """.stripMargin
        spark.sql(sql).show()
        spark.stop()
    }
}
