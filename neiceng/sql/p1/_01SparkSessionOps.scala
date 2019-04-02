package com.desheng.bigdata.spark.scala.sql.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * 如何去构建一个SparkSession对象
  *
  */
object _01SparkSessionOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        //sparksession的构建
        val spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkSessionOps")
                .getOrCreate()

        val df:DataFrame = spark.read.json("file:///E:/data/spark/sql/people.json")
        println("显示二维表中的数据")
        df.show
        println("输出二维表的元数据信息")
        df.printSchema()
        println("查询表中的姓名和年龄数据")
        df.select("name", "age").show
        df.select(new Column("name"), new Column("age"), new Column("height")).show()
        println("复杂查询")
        println("===年龄大于20的人，给每个人的年龄+10，统计不同年龄的人数有多少个")
        df.select(new Column("name"), new Column("age").>(20).as("bigger_20")).show()
        //where age > 20
        df.select(new Column("name"), new Column("age").as("bigger_20")).where("age > 20").show()
        println("给每个人的年龄+10")
        df.select(new Column("name"), new Column("age").+(10).as("add_20")).show()
        println("统计不同年龄的人数有多少个")//select age, count(1) from people group by age

        df.select("age").groupBy("age").count().show()
        println("SQL操作的查询")//要想使用sql操作，需要将dataframe或者dataset注册成为一张临时表
        /**
          * 在创建临时表的时候可以加Global，也可以不用加
          * 加：在当前的Application中有效
          * 不加：作用范围在当前SparkSession中
          */
        df.createOrReplaceTempView("people")
        val sql =
            """
              |select
              | age,
              | count(1) countz
              |from people
              |group by age
              |order by countz desc
            """.stripMargin
        spark.sql(sql).show()
        spark.stop()
    }
}
