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
object _03SparkSQLDatasetOps {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("SparkSQLDatasetOps")
            .getOrCreate()
        datasetOps(spark)

        spark.stop()
    }

    def datasetOps(spark: SparkSession) = {
        val list = new util.ArrayList[Student]()
        list.add(new Student("李肖文", 18, 196.8))
        list.add(new Student("杨续峰", 20, 176.8))
        list.add(new Student("胡顺利", 16, 168.88))
        list.add(new Student("郑  垚", 20, 183.8))
        //在构建dataset的时候需要引入隐式转换
        import spark.implicits._
        var personDS:Dataset[Student] = spark.createDataset(list)
        personDS.show()
        val studentRDD = spark.sparkContext.parallelize(JavaConversions.asScalaBuffer(list))
        personDS = spark.createDataset(studentRDD)
        personDS.show()
        personDS.rdd.foreach(println)

    }
}
case class Student(name:String, age:Int, height:Double)
