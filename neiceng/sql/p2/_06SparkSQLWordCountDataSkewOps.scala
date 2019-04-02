package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object _06SparkSQLWordCountDataSkewOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLWordCountDataSkewOps")
            .master("local[*]")
            .getOrCreate()

        val list = List(
            "hello you",
            "hello me",
            "hello you",
            "hello you",
            "hello you",
            "me you",
            "hai you",
            "hello shit",
            "me you"
        )
        import spark.implicits._
        val df = spark.sparkContext.parallelize(list).toDF("line")
//        df.show()
        println("使用sql完成wordcount的统计")
        df.createTempView("test")
        println("step1、完成单词提取")
        var sql =
            """
              |select
              |   explode(split(line, '\\s+')) word
              |from test
            """.stripMargin
        spark.sql(sql).show
        println("step2、为key添加随机前缀")
        sql =
            """
              |select
              |  concat_ws("_", cast(floor(rand() * 3) as string), t1.word) prefix_word
              |from (
              | select
              |   explode(split(line, '\\s+')) word
              | from test
              |) t1
            """.stripMargin
        spark.sql(sql).show

        println("step3、基于随机前缀进行局部聚合")
        sql =
            """
              |select
              |  concat_ws("_", cast(floor(rand() * 3) as string), t1.word) prefix_word,
              |  count(1) countz
              |from (
              | select
              |   explode(split(line, '\\s+')) word
              | from test
              |) t1
              |group by prefix_word
            """.stripMargin
        spark.sql(sql).show
        println("step4、去掉随机前缀")
        sql =
            """
              |select
              | substr(t2.prefix_word, instr(t2.prefix_word, "_") + 1) unprefix_word,
              | t2.countz
              |from (
              | select
              |     concat_ws("_", cast(floor(rand() * 3) as string), t1.word) prefix_word,
              |     count(1) countz
              | from (
              |     select
              |     explode(split(line, '\\s+')) word
              |     from test
              | ) t1
              | group by prefix_word
              |) t2
            """.stripMargin
        spark.sql(sql).show

        println("step5、最终结果全局聚合")
        sql =
            """
              |select
              | substr(t2.prefix_word, instr(t2.prefix_word, "_") + 1) unprefix_word,
              | sum(t2.countz) as count
              |from (
              | select
              |     concat_ws("_", cast(floor(rand() * 3) as string), t1.word) prefix_word,
              |     count(1) countz
              | from (
              |     select
              |     explode(split(line, '\\s+')) word
              |     from test
              | ) t1
              | group by prefix_word
              |) t2
              |group by unprefix_word
            """.stripMargin
        spark.sql(sql).show
        spark.stop()
    }
}
