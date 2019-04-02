package com.desheng.bigdata.spark.scala.core.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * 常见的Transformation操作
  * 1)map：将集合中每个元素乘以7
  * 2)filter：过滤出集合中的奇数
  * 3)flatMap：将行拆分为单词
  * 4)sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
  * 5)union：返回一个新的数据集，由原数据集和参数联合而成
  * 6)groupByKey：对数组进行 group by key操作 慎用
  * 7)reduceByKey：统计每个班级的人数
  * 8)join：打印关联的组合信息
  * 9)sortByKey：将学生身高进行排序
  */
object _02SparkTransformationOps {


    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("TransformationOps")
            .setMaster("local[*]")
        val sc = new SparkContext(conf)
        val list = 1 to 7
        val listRDD:RDD[Int] = sc.parallelize(list)
                mapOps(sc, listRDD)
        //        filterOps(sc, listRDD)
        //        flatMapOps(sc)
//        sampleOps(sc)
//        unionOps(sc)
//        gbkOps(sc)
//        rbkOps(sc)
//        joinOps(sc)
//        sbkOps(sc)
//        sbOps(sc)
        sc.stop()
    }

    /**
      * sortBy--->排序
      *     stu
      *         id name height
      */
    def sbOps(sc: SparkContext): Unit = {
        val stuList = List(
            "1^吕航^187",
            "2^吴力强^178.5",
            "3^彭痰^178.0",
            "4^小俊俊^168.0",
            "5^小萌^165.5"
        )
        val stuRDD:RDD[String] = sc.parallelize(stuList)
        //就要按照学生的身高进行排序
        val sortedRDD:RDD[String] = stuRDD.sortBy(
            (info:String) => {
                val height:Double = info.substring(info.lastIndexOf("^") + 1).toDouble
                height
            },
            ascending = false,
            numPartitions = 1
        )(new Ordering[Double](){
            override def compare(xHeight: Double, yHeight: Double) = {
                yHeight.compareTo(xHeight)
            }
        },
        ClassTag.Double.asInstanceOf[ClassTag[Double]]
        )

        sortedRDD.foreach(println)
    }
    /**
      * 按照单词个数进行排序
      * sortByKey:按照key进行排序，是一个分区内的局部排序
      *     [K, V]-->[K, V]
      * 想全局排序，只能将partition设置为1
      * @param sc
      */
    def sbkOps(sc: SparkContext): Unit = {
        val list = List(
            "liu chang shi chang chang",
            "jun jun song",
            "zheng yao yao"
        )
        val listRDD:RDD[String] = sc.parallelize(list)
        val wordsRDD:RDD[String] = listRDD.flatMap(_.split("\\s+"))
        val pairsRDD:RDD[(String, Int)] = wordsRDD.map((_, 1)).reduceByKey(_+_)
        println("partition: " + pairsRDD.getNumPartitions)
        pairsRDD.map{case (word, count) => (count, word)}
            .sortByKey(ascending = false, numPartitions = 1)//排序的参数
            .foreach(println)
    }
    /**
      * join：表的关联
      *     左表A：[K, V]
      *     右表B：[K, W]
      *     交叉连接：across join --->没有连接条件on --->笛卡尔积
      *     内连接  : inner join --->等值连接：返回左右两张表的交集
      *         A.join(B)--->[K, (V, W)]
      *     外连接  ： outer join --->不等值连接
      *        左外连接：左表A left outer join 右表B，返回左表A的所有数据，右表B向的匹配数据，匹配不了返回null
      *         A.leftOuterJoin(B)--->[K, (V, Option[W])]
      *        右外连接：左表A right outer join 右表B，返回右表B的所有数据，左表A的匹配数据，匹配不了返回null
      *         A.rightOuterJoin(B)--->[K, (Option[V], W)]
      *        全连接：左表A full outer join 右表B <==> 左外连接+右外连接
      *         A.fullOuterJoin(B)--->[K, (Option[V], Option[W])]
      *
      * 学生表stu
      *     id name pid
      * 地区
      *     pid  province
      */
    def joinOps(sc: SparkContext): Unit = {
        val provinceList = List(
            "1,陕西",
            "2,甘肃",
            "3,广东",
            "4,河北",
            "5,山东"
        )
        val stuList = List(
            "1,曹凯路,1",
            "2,吕萌,2",
            "3,胡俊杰,3",
            "4,杨洋,1",
            "5,刘世昌,6"
        )
        val provinceRDD:RDD[String] = sc.parallelize(provinceList)
        val stuRDD:RDD[String] = sc.parallelize(stuList)
        //省份
        val pid2ProvinceRDD:RDD[(Int, String)] = provinceRDD.map(line => {
            val fields = line.split(",")
            val pid = fields(0).toInt
            val province = fields(1)
            (pid, province)
        })
        //学生
        val sid2Info:RDD[(Int, (Int, String))] = stuRDD.map(line => {
            val fields = line.split(",")
            val sid = fields(0).toInt
            val name = fields(1)
            val pid = fields(2).toInt
            (pid, (sid, name))
        })
        //学生的所有信息
        val stuJoinInfo:RDD[(Int, ((Int, String), String))] = sid2Info.join(pid2ProvinceRDD)
        println("----------内连接之后的数据--------------")
//        stuJoinInfo.foreach(t => {
//            println(s"pid:${t._1}\tsid:${t._2._1._1}\tname:${t._2._1._2}\tprovince:${t._2._2}")
//        })
        stuJoinInfo.foreach{case (pid, ((sid, name), province)) => {
            println(s"pid:${pid}\tsid:${sid}\tname:${name}\tprovince:${province}")
        }}
        println("----------外连接之后的数据--------------")
        val stuLOJoinInfo:RDD[(Int, ((Int, String), Option[String]))] = sid2Info
            .leftOuterJoin(pid2ProvinceRDD)
        stuLOJoinInfo.foreach{case (pid, ((sid, name), option)) => {
            println(s"pid:${pid}\tsid:${sid}\tname:${name}\tprovince:${option.getOrElse("外国")}")
        }}
        println("----------全连接之后的数据--------------")
        val stufOJoinInfo:RDD[(Int, (Option[(Int, String)], Option[String]))] = sid2Info
            .fullOuterJoin(pid2ProvinceRDD)
        stufOJoinInfo.foreach{case (pid, (stuOption, provinceOption)) => {
            println(s"pid:${pid}\tsid:${stuOption.getOrElse(null)}\tprovince:" +
                s"${provinceOption.getOrElse(null)}")
        }}
    }

    /**
      * reduceByKey: 按照key进行reduce操作
      *     [K, V] --->reduceByKey---> [K, V]
      */
    def rbkOps(sc: SparkContext): Unit = {
        val list = List(
            "liu shi chang chang",
            "jun jun song",
            "zheng yao yao"
        )
        val listRDD:RDD[String] = sc.parallelize(list)
        val wordsRDD:RDD[String] = listRDD.flatMap(_.split("\\s+"))
        val pairsRDD:RDD[(String, Int)] = wordsRDD.map((_, 1))
        pairsRDD.reduceByKey(_+_).foreach(println)
    }

    /**
      * groupByKey:按照key执行sql中的group By的操作，简言之，就将key相同的value分组。
      * [K, V]--groupByKey--->[K, Iterable[V]]
      * @param sc
      */
    def gbkOps(sc: SparkContext) = {
        val list = List(
            "liu shi chang chang",
            "jun jun song",
            "zheng yao yao"
        )
        val listRDD:RDD[String] = sc.parallelize(list)
        val wordsRDD:RDD[String] = listRDD.flatMap(_.split("\\s+"))
        val pairsRDD:RDD[(String, Int)] = wordsRDD.map((_, 1))
        val gbkRDD:RDD[(String, Iterable[Int])] = pairsRDD.groupByKey()
        gbkRDD.foreach(println)
        val retRDD:RDD[(String, Int)] = gbkRDD.map{case(key, values) => (key, values.size)}
        retRDD.foreach(println)
    }

    def unionOps(sc: SparkContext) = {
        val list1 = List(1, 3, 5, 7, 9)
        val list2 = List(2, 4, 6, 9, 10)
        val listRDD1:RDD[Int] = sc.parallelize(list1)
        val listRDD2:RDD[Int] = sc.parallelize(list2)
        val unionRDD:RDD[Int] = listRDD1.union(listRDD2)

        unionRDD.foreach(println)
    }
    /**
      * 4)sample(withReplacement, frac, seed): 根据给定的随机种子seed，随机抽样出数量为frac的数据
      *     withReplacement: 抽样是否有放回
      *     fraction：抽样比例
      *     seed：随机数生成器
      *   spark中的这个sample抽样算子是非准确的
      *   sample进程用在查看rdd中key的分布情况，一个最经典的应用案例就在处理spark的数据倾斜dataskew
      * @param sc
      */
    def sampleOps(sc: SparkContext) = {
        val list = 1 to 100000
        val listRDD:RDD[Int] = sc.parallelize(list)
        println("源RDD的size：" + listRDD.count())
        val sampleRDD:RDD[Int] = listRDD.sample(false, 0.01)
        println("sampleRDD的size：" + sampleRDD.count())
    }
    /**
      * 3)flatMap：将行拆分为单词
      * @param sc
      */
    def flatMapOps(sc: SparkContext) = {
        val list = List(
            "zheng yao yao",
            "hu jun jie jie",
            "zhao zeng qiang"
        )
        val listRDD:RDD[String] = sc.parallelize(list)
        println("原集合元素：")
        listRDD.collect.foreach(println)
        val retRDD:RDD[String] = listRDD.flatMap(line => line.split("\\s+"))
        println("\r\n变换之后的元素：")
        retRDD.foreach(println)
    }
    /**
      * 2)filter：过滤出集合中的奇数
      */
    def filterOps(sc:SparkContext, listRDD:RDD[Int]): Unit = {
        println("原集合元素：")
        listRDD.collect.foreach(tt => print(tt + "\t"))
        val retRDD:RDD[Int] = listRDD.filter(num => num % 2 == 0)
        println("\r\n变换之后的元素：")
        retRDD.foreach(t => print(t + "\t"))
    }
    /**
      * 1)map：将集合中每个元素乘以7
      */
    def mapOps(sc: SparkContext, listRDD:RDD[Int]): Unit = {
        println("原集合元素：")
        var bs = 7
        listRDD.collect.foreach(tt => print(tt + "\t"))
        val retRDD:RDD[Int] = listRDD.map(num => {
                bs += 1
                num * bs
        })
        println("\r\n变换之后的元素：")
        println("bs====" + bs)
        retRDD.foreach(t => print(t + "\t"))
    }
}
