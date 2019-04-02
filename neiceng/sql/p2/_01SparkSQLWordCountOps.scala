package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object _01SparkSQLWordCountOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLWordCount")
            .master("local[*]")
//            .enableHiveSupport()//有他可以提供hive的相关操作
            .getOrCreate()

        val list = List(
            "hello you",
            "hello me",
            "hello you",
            "hello you",
            "me you"
        )
        import spark.implicits._
        val df = spark.sparkContext.parallelize(list).toDF("line")
        df.show()
        println("使用sql完成wordcount的统计")
        df.createTempView("test")
        /**
          * select
          *     word,
          *     count(1)
          * from t
          * group by word
          */
        println("step1、完成单词提取")
        var sql =
            """
              |select
              | split(line, '\\s+')
              |from test
            """.stripMargin
        spark.sql(sql).show()
        println("step2、完成列转行")
        sql =
            """
              |select
              | explode(split(line, '\\s+')) word
              |from test
            """.stripMargin
        spark.sql(sql).show
        println("step3、完成wordcount")
        sql =
            """
              |select
              | tmp.word,
              | count(tmp.word) as countz
              |from (
              | select
              |   explode(split(line, '\\s+')) word
              | from test
              |) tmp
              |group by tmp.word
              |order by countz desc
            """.stripMargin
        spark.sql(sql).show
        spark.stop()
    }
}
