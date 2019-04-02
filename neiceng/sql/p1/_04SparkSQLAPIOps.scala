package com.desheng.bigdata.spark.scala.sql.p1

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.beans.BeanProperty
import scala.collection.JavaConversions

/**
  * Dataset的构建
  */
object _04SparkSQLAPIOps {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("SparkSQLDatasetOps")
            .getOrCreate()
        sqlAPIOps(spark)

        spark.stop()
    }
    def sqlAPIOps(spark: SparkSession) = {
        //rdd-2-dataframe
        val list = new util.ArrayList[Person]()
        list.add(new Person("李肖文", 18, 196.8))
        list.add(new Person("杨续峰", 20, 176.8))
        list.add(new Person("胡顺利", 16, 168.88))
        list.add(new Person("郑  垚", 20, 183.8))
        import spark.implicits._
        var personDF: DataFrame = spark.createDataFrame(list, classOf[Person])
        personDF.show()
        /*
            dataframe-2-dataset --->as
            因为case class没有getter/setter等等操作，也没有对应类体，所以无法直接封装数据
            也就无法通过反射的方式来获取其元数据
            根据提示，将case class 切换为原来的普通的javabean，但是该javabean的类型需要是Product类型
            重新改造了Person，扩展了Product复写了其中三个方法
         */
        val pDS = personDF.as[Person]
        pDS.show()
        //rdd-2-dataset
        val personRDD:RDD[Person] = spark.sparkContext.parallelize(JavaConversions.asScalaBuffer(list))
        val rdd2DS = personRDD.toDS()
        rdd2DS.printSchema()
        rdd2DS.show()
        println("--------------------------------------")
        println("dataset-2-dataframe")
        pDS.toDF().show()
        pDS.rdd.foreach(p => println(p.getName))

    }
}
