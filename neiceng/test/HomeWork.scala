package com.desheng.bigdata.spark.scala.test

import scala.util.control.Breaks._
object HomeWork {


    def main(args: Array[String]): Unit = {
        val str1 = "ertonbadsvfafdnbasfd";
        val str2 = "z";
        val sub = getMaxSubStr(str1, str2)
        println("sub: " + sub)
    }

    def getMaxSubStr(str1: String, str2: String):String = {
        val max = if(str1.length() > str2.length())  str1 else str2
        val min = if(str1.length() < str2.length())  str1 else str2

        var sub:String = null
        breakable {
            for (i <- 0 until min.length) {
                var start = 0
                var end = min.length - i
                while (start < end && end <= min.length) {
                    sub = min.substring(start, end)
                    if (max.contains(sub)) {
                        break
                    } else {
                        start += 1
                        end += 1
                    }
                }
            }
        }
        if(max.contains(sub)) sub else null
    }
}
