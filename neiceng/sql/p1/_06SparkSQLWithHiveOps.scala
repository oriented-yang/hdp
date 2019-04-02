package com.desheng.bigdata.spark.scala.sql.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * SparkSQL整合Hive的操作
  * 有两张hive表：
  *     teacher_info
  *         name,height
  *     teacher_basic
  *         name,
  *         age
  *         married
  *         children
  *     进行关联查询，查询出所有的teacher信息并且保存在hive的表中
  *     select
  *         i.name,
  *         b.age,
  *         b.married,
  *         b.children,
  *         i.height
  *     from teacher_info i
  *     left join teacher_basic b on i.name = b.name
  * SparkSQL和Hive进行整合需要做如下的配置：
  *     1、需要将hive-site.xml添加到classpath下面
  *         将hive-site.xml放到项目的resources目录下面
  *     2、将mysql的驱动jar包拷贝到spark的classpath下面
  *         直接将jar扔到spark的jars目录下面
  *         cp ~/app/hive/lib/mysql-connector-java-5.1.39.jar ~/app/spark/jars/
  *         scp ~/app/spark/jars/mysql-connector-java-5.1.39.jar bigdata@bigdata02:/home/bigdata/app/spark/jars/
  *         scp ~/app/spark/jars/mysql-connector-java-5.1.39.jar bigdata@bigdata03:/home/bigdata/app/spark/jars/
  */
object _06SparkSQLWithHiveOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .appName("SparkSQLLoadAndSave")
            .enableHiveSupport()//有他可以提供hive的相关操作
            .getOrCreate()
        //创建一个数据库
        spark.sql("create database db_1810")

        val createInfoSQL =
            """
              |create table `db_1810`.`teacher_info` (
              |   name string,
              |   height int
              |) row format delimited
              |fields terminated by ','
            """.stripMargin
        spark.sql(createInfoSQL)

        val createBasicSQL =
            """
              |create table `db_1810`.`teacher_basic` (
              |   name string,
              |   age int,
              |   married boolean,
              |   children int
              |) row format delimited
              |fields terminated by ','
            """.stripMargin
        spark.sql(createBasicSQL)
        //加载数据
        val loadInfoSQL = "load data inpath 'hdfs://ns1/input/spark/teacher_info.txt' into table `db_1810`.`teacher_info`"
        val loadBasicSQL = "load data inpath 'hdfs://ns1/input/spark/teacher_basic.txt' into table `db_1810`.`teacher_basic`"
        spark.sql(loadInfoSQL)
        spark.sql(loadBasicSQL)
        //执行关联查询
        val sql =
            """
              |select
              |  i.name,
              |  b.age,
              |  b.married,
              |  b.children,
              |  i.height
              |from `db_1810`.`teacher_info` i
              |left join `db_1810`.`teacher_basic` b on i.name = b.name
            """.stripMargin
        val joinedDF = spark.sql(sql)

        //将结果落地到Hive中的表teacher中
        joinedDF.write.saveAsTable("`db_1810`.`teacher`")
        spark.stop()
    }
}
