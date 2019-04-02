package com.desheng.bigdata.spark.scala.sql.p2

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
  * 自定义聚合函数
  * avg(值) = sum / count
  * 我们在学习combineByKey的时候计算也是三个函数：
  *     createCombiner  --->初始化
  *     mergeValue      --->分区内合并
  *     mergeCombiner   --->分区间合并
  *     初始化过程----
  * var sum = 0
  * var count = 0
  *     合并过程-----
  * for(i <- 0 to 10) {
  *     sum += i
  *     count += 1
  * }
  *     ----最终结果
  * val avg = sum / count
  *
  *
  */
class MyAVGUDAF extends UserDefinedAggregateFunction {

    //该聚合函数输入参数的元数据信息
    override def inputSchema: StructType = StructType(
        List(
            StructField("score", DataTypes.IntegerType, false)
        )
    )

    //该聚合函数在统计过程中的临时结果的元数据信息
    override def bufferSchema: StructType = StructType(
        List(
            StructField("sum", DataTypes.IntegerType, false),
            StructField("count", DataTypes.IntegerType, false)
        )
    )

    //该聚合函数输出结果的数据类型
    override def dataType: DataType = DataTypes.DoubleType

    override def deterministic: Boolean = true

    /*
        初始化
        var sum = 0
        var count = 0
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, 0)//初始化sum=0
        buffer.update(1, 0)//初始化count=0
    }

    /*
        分区内更新
          * for(i <- 0 to 10) {
          *     sum += i
          *     count += 1
          * }
     */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer.update(0, buffer.getInt(0) + input.getInt(0))// sum += i
        buffer.update(1, buffer.getInt(1) + 1)//count += 1
    }
    //分区间聚合
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))// sum += i1
        buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1))//count += count1
    }

    //聚合结果 sum / count
    override def evaluate(buffer: Row): Any = {
        buffer.getInt(0).toDouble / buffer.getInt(1)
    }
}
