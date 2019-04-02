package com.desheng.bigdata.spark.scala.optimization.dev

import java.sql.DriverManager

import com.desheng.bigdata.spark.java.util.ConnectionPool
import com.desheng.bigdata.spark.scala.optimization.dev._02ShuffleOptimizationOps.joinUnShuffleOps
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 使用高性能的Spark算子
  */
object _03HighPerformOperationOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.spark-project").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setAppName("HighPerformOperation")
            .setMaster("local[2]")
        val sc = new SparkContext(conf)
//        mapPartitionOps(sc)
//        foreachPartitionOps1(sc)
//        foreachPartitionOps2(sc)
//        foreachPartitionOps3(sc)
//        foreachPartitionOps4(sc)
        coalesceOps(sc)
        sc.stop()
    }
    /*
        在执行完filter操作之后进行重分区操作
        重分区有两个操作
            1、coalesce，默认shuffle=false-->窄依赖操作(1:N)
                适合分区减少的操作
            2、repartition，默认shuffle=true-->宽依赖操作
                适合分区增大的操作
                coalesce(num, shuffle=true)
     */
    def coalesceOps(sc: SparkContext): Unit = {
        val list = 1 to 10
        val listRDD = sc.parallelize(list)
        val filteredRDD = listRDD.filter(_ % 2 != 0)
        val rdd = filteredRDD.repartition(1)//分区个数
//        val count = rdd.count()
//        println("count = " + count)
        rdd.foreach(println)
    }
    //分批次写入数据库 每次5条记录
    def foreachPartitionOps4(sc: SparkContext): Unit = {
        val ret = getInfo(sc)
        println("partitions: " + ret.getNumPartitions)
        ret.foreachPartition(partition => {
            if(!partition.isEmpty) {
                val connection = ConnectionPool.getConnection
                val sql =
                    """
                      |insert into test values(?, ?)
                    """.stripMargin
                val ps = connection.prepareStatement(sql)
                var cur = 0
                partition.foreach { case (word, count) => {
                    ps.setString(1, word)
                    ps.setInt(2, count)
                    cur += 1
                    ps.addBatch()
                    if(cur % 5 == 0) {
                        ps.executeBatch()
                    }
                }}
                if(cur % 5 != 0) {
                    ps.executeBatch()
                }
                ps.close()
                ConnectionPool.release(connection)
            }
        })
    }
    //使用数据库连接池
    def foreachPartitionOps3(sc: SparkContext): Unit = {
        val ret = getInfo(sc)
        ret.foreachPartition(partition => {
            if(!partition.isEmpty) {
                val connection = ConnectionPool.getConnection
                val sql =
                    """
                      |insert into test values(?, ?)
                    """.stripMargin
                val ps = connection.prepareStatement(sql)
                partition.foreach { case (word, count) => {
                    ps.setString(1, word)
                    ps.setInt(2, count)
                    ps.addBatch()
                }}
                ps.executeBatch()
                ps.close()
                ConnectionPool.release(connection)
            }
        })
    }

    //因为foreach的性能非常低下，使用foreachPartition去代替它
    def foreachPartitionOps2(sc: SparkContext): Unit = {
        val ret = getInfo(sc)
        classOf[com.mysql.jdbc.Driver]
        ret.foreachPartition(partition => {
            if(!partition.isEmpty) {
                val url = "jdbc:mysql://localhost:3306/test"
                val connection = DriverManager.getConnection(url, "root", "sorry")
                val sql =
                    """
                      |insert into test values(?, ?)
                    """.stripMargin
                val ps = connection.prepareStatement(sql)
                partition.foreach { case (word, count) => {
                    ps.setString(1, word)
                    ps.setInt(2, count)
                    ps.addBatch()
                }}
                ps.executeBatch()
                ps.close()
                connection.close()
            }
        })
    }
    //create table test(word varchar(20), `count` int);
    def foreachPartitionOps1(sc: SparkContext): Unit = {
        val ret = getInfo(sc)
        //        Class.forName("")
        classOf[com.mysql.jdbc.Driver]
        ret.foreach{case (word, count) => {
            val url = "jdbc:mysql://localhost:3306/test"
            val connection = DriverManager.getConnection(url, "root", "sorry")
            val sql =
                """
                  |insert into test values(?, ?)
                """.stripMargin
            val ps = connection.prepareStatement(sql)
            ps.setString(1, word)
            ps.setInt(2, count)
            ps.execute()
            ps.close()
            connection.close()
        }}
    }

    def getInfo(sc:SparkContext):RDD[(String, Int)] = {
        val lines:RDD[String] = sc.textFile("file:///E:/data/hello.txt")
        val words:RDD[String] = lines.flatMap(line => line.split("\\s+"))
        val pairs:RDD[(String, Int)] = words.map(word => (word, 1))
        pairs.reduceByKey(_+_)
    }

    def  mapPartitionOps(sc: SparkContext): Unit = {
        val provinces = List(
            "1,陕西",
            "2,甘肃",
            "3,广东",
            "4,河北",
            "5,山东"
        ).map(line => {
            val fields = line.split(",")
            val pid = fields(0).toInt
            val province = fields(1)
            (pid, province)
        }).toMap
        val pBC:Broadcast[Map[Int, String]] = sc.broadcast(provinces)
        val stuList = List(
            "1,曹凯路,1",
            "2,吕萌,2",
            "3,胡俊杰,3",
            "4,杨洋,1",
            "5,刘世昌,6"
        )
        val stuRDD:RDD[String] = sc.parallelize(stuList)

        //学生 infos代表的是一个分区内的数据
        val sid2Info:RDD[(String, (Int, String))] = stuRDD.mapPartitions(infos => {
            val ab = ArrayBuffer[(String, (Int, String))]()
            for(info <- infos) {
                val fields = info.split(",")
                val sid = fields(0).toInt
                val name = fields(1)
                val pid = fields(2).toInt
                val province = pBC.value.getOrElse(pid, "北京")
                ab.append((province, (sid, name)))
            }
            ab.iterator
        })
        sid2Info.foreach(println)
    }
}
