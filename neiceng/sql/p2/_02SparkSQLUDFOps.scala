package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * UDF
  *   步骤：
  *     1、要么扩展类或者接口，然后复写其中的方法，要么直接创建方法
  *     2、进行注册
  *     3、直接进行调用
  *
  *  udf非常简单，通过模拟字符串长度的函数length来学习udf的操作
  */
object _02SparkSQLUDFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLUDAFOps")
            .master("local[*]")
            .getOrCreate()
        //step2、注册
//        spark.sqlContext.udf.register()//老版本的注册方式
//        spark.udf.register("myLen", str => strLen(str))
        spark.udf.register[Int, String]("myLen", strLen)//简写版本

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
        df.createTempView("test")
        //step3、调用
        val sql =
            """
              |select
              | line,
              | length(line) offical_len,
              | myLen(line) myLen
              |from test
            """.stripMargin
        spark.sql(sql).show()
        spark.stop()
    }
    //step 1、自定义函数，完成udf逻辑
    def strLen(str:String) = str.length
}
