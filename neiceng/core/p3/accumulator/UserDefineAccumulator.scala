package com.desheng.bigdata.spark.scala.core.p3.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 在累加器定义的时候有两个泛型参数
  * IN：累加的时候输入到累加器的数据的类型
  * OUT：该累加器中经过累加之后的数据的类型
  * 计算word出现的次数
  */
class UserDefineAccumulator extends
    AccumulatorV2[String, mutable.Map[String, Int]] {
    var map = mutable.Map[String, Int]()
    //当前累加器是否有初始值，对于是数字累加器，初始值就为0，对于集合的初始值就是空集合
    override def isZero: Boolean = true

    /*
        因为spark作业是在多个task上面执行的，所以需要多个累加器在不同的task上面运行，
        进而就需要做累加器的拷贝
     */
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
        val uda = new UserDefineAccumulator
        uda.map = map
        uda
    }

    //重置当前累加器
    override def reset(): Unit = map.clear()
    //获取当前累加器的值
    override def value: mutable.Map[String, Int] = map

    //在累加器中进行累加操作
    override def add(word: String): Unit = {
//        val option = map.get(word)
//        if(option.isDefined) {
//            map.put(word, 1 + option.get)
//        } else {
//            map.put(word, 1)
//        }
        map.put(word, map.getOrElse(word, 0) + 1)
    }

    //两个task中的累加器进行合并操作
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
        val otherAccVal:mutable.Map[String, Int] = other.value
//        map.++:(otherAccVal)
        for((word, count) <- otherAccVal) {
            map.put(word, map.getOrElse(word, 0) + count)
        }
    }
}
