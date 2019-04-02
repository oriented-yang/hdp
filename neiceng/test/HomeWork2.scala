package com.desheng.bigdata.spark.scala.test

import com.desheng.bigdata.spark.scala.core.p1._01SparkScalaWordCount
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 1	Java高级	北苑		信息安全,大数据,架构	闲徕互娱	20k-40k		本科	经验5-10年	游戏		不需要融资
  * 2	Java高级	北苑		JavaEE,架构	        闲徕互娱	20k-40k		本科	经验5-10年	游戏		不需要融资
  **
  *字段：
  *id	job		addr		tag			company		salary		edu	exp		type		level
  **
  *中文解释
  *id	工作岗位	地址		标签			公司		薪资		学历	经验		类型		融资级别
  * 3、求出不同标签的招聘数量和公司数量（只输出招聘需求最大的50个标签）
  **
  *结果样式：
  *高级,5232,1414
  *金融,4865,995
  *资深,3717,1080
  *Java,3531,1154
  *大数据,3375,831
  *........
  */
object HomeWork2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)

        val conf = new SparkConf()
        //master
        conf.setAppName("HomeWork2")
        conf.setMaster("local")
        val sc = new SparkContext(conf)
        //加载本地的文件
        val lines:RDD[String] = sc.textFile("file:///E:/work/1810-bd/lagou.txt")
        //标签对应的公司数量--招聘的数量
        val tagCompany = lines.flatMap(line => {
            val fields = line.split("\\^")
            val tags = fields(3).split(",")//信息安全,大数据,架构
            val company = fields(4)
            val ab = new ArrayBuffer[((String, String), Int)]()
            for(tag <- tags) {
                /**
                  * 如果用<tag, company>这就代表了一个标签的一次招聘活动
                  * 同时这组tuple中也有重复的同一个标签对应的同一个公司
                  */
                ab.append(((tag, company), 1))
            }
            ab
        })
        /**
          * (tag---->company, 总数)
          * 架构--ali, 2
          * 架构--baidu, 3
          *
          * 架构, 5--->标签对应招聘的数量
          *
          * 架构, 2--->标签对应公司的数量
          *
          */
        val tagCompanyCounts = tagCompany.reduceByKey(_+_)
        //计算招聘的数量
//        val tag2ZPCount:RDD[(String, Int)] = tagCompanyCounts.map{case ((tag, company), zpc) => {
//            (tag, zpc)
//        }}.reduceByKey(_+_)
//        //计算公司的数量
//        val tag2CompanyCount:RDD[(String, Int)] = tagCompanyCounts.map{case ((tag, company), zpc) => {
//            (tag, 1)
//        }}.reduceByKey(_+_)
        //合并上述的编码
        val retRDD:RDD[(String, (Int, Int))] = tagCompanyCounts.map{case ((tag, company), zpc) => {
            (tag, (zpc, 1))
        }}.reduceByKey{case ((zpc1, c1), (zpc2, c2)) => {
            //(招聘，公司)
            (zpc1 + zpc2, c1 + c2)
        }}
        //招聘降序，公司也降序
        val result:Array[(String, (Int, Int))] = retRDD.takeOrdered(10)(new Ordering[(String, (Int, Int))](){
            override def compare(x: (String, (Int, Int)), y: (String, (Int, Int))) = {
                var ret = y._2._1.compareTo(x._2._1)
                if(ret == 0) {
                    ret = y._2._2.compareTo(x._2._2)
                }
                ret
            }
        })

        result.foreach{case (tag, (zpc, company)) => {
            println(s"${tag}:\t${zpc}\t${company}")
        }}
        sc.stop
    }
}
