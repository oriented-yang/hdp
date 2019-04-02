package com.desheng.bigdata.spark.scala.sql.p1

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.beans.BeanProperty
import scala.collection.JavaConversions

/**
  * DataFrame的构建
  *     有两种方式进行构建：
  *     1、通过javabean的反射构建
  *     2、动态编程的方式来构建
  */
object _02SparkSQLDataFrameOps {


    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)


        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("SparkSQLDataFrameOps")
            .getOrCreate()
//        dataFrameByReflection(spark)
        dataFrameByDynamic(spark)

        spark.stop()
    }

    /**
      * 动态编程的方式和反射的方式的区别
      * 1、可以通过反射的方式来获取对象身上的字段等元数据信息
      * 2、动态编程的这种方式只能自身来进行指定元数据信息
      * @param spark
      */
    def dataFrameByDynamic(spark: SparkSession) = {
        val rowList = new util.ArrayList[Row]()
        rowList.add(Row("李肖文", 18, 196.8))
        rowList.add(Row("杨续峰", 20, 176.8))
        rowList.add(Row("胡顺利", 16, 168.88))
        rowList.add(Row("郑  垚", 20, 183.8))
        val schema = StructType(
            List(
                StructField("name", DataTypes.StringType, false),
                StructField("age", DataTypes.IntegerType, false),
                StructField("height", DataTypes.DoubleType, false)
            )
        )
        var pDF = spark.createDataFrame(rowList, schema)
        pDF.show()

        val rowRDD:RDD[Row] = spark.sparkContext.parallelize(JavaConversions.asScalaBuffer(rowList))
        pDF = spark.createDataFrame(rowRDD, schema)
        pDF.show()
    }
    /**
      * 通过javabean的反射构建
      * @param spark
      */
    def dataFrameByReflection(spark: SparkSession) = {
        val list = new util.ArrayList[Person]()
        list.add(new Person("李肖文", 18, 196.8))
        list.add(new Person("杨续峰", 20, 176.8))
        list.add(new Person("胡顺利", 16, 168.88))
        list.add(new Person("郑  垚", 20, 183.8))

        var personDF:DataFrame = spark.createDataFrame(list, classOf[Person])
        personDF.show()

        /*
            通过rdd进行构建-->sparkContext
            JavaConversions工具类用于java和scala集合之间的转换
         */
        val scalaList = JavaConversions.asScalaBuffer(list)

        val personRDD:RDD[Person] = spark.sparkContext.parallelize(scalaList)

        personDF = spark.createDataFrame(personRDD, classOf[Person])
        personDF.show()

    }
}
class Person extends Serializable with Product {
    @BeanProperty var name:String = _
    @BeanProperty var age:Int = _
    @BeanProperty var height:Double = _
    def this(name:String, age:Int, height:Double) {
        this()
        this.name = name
        this.age = age
        this.height = height
    }

    /**
      * 传入属性索引来获取对应的属性值
      */
    override def productElement(n: Int) = {
        n match {
            case 0 => name
            case 1 => age
            case 2 => height
        }
    }
    /**
      * 当前对象的属性的个数
      */
    override def productArity = 3
    //是否可以进行对象的判断
    override def canEqual(that: Any) = {
        that match {
            case p:Person => true
            case _ => false
        }
    }
}
