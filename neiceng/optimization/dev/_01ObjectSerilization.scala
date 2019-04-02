package com.desheng.bigdata.spark.scala.optimization.dev

import java.io._

object _01ObjectSerilization {
    def main(args: Array[String]): Unit = {
        val file = "data/objs.txt"
        writeObj2File(file)
        readObjFromFile(file)
    }

    /**
      * EOFException
      * 两种解决方案：
      * 1、添加特殊的结束标志
      * 2、因为读取一个对象肯定不会出错，所以想个办法吧所有的对象变成一个
      */
    def readObjFromFile(file:String): Unit = {
        val oips = new ObjectInputStream(new FileInputStream(file))
//        var obj:Any = null
//        while((obj = oips.readObject()) != -1) {
//            obj match {
//                case p: Person => {
//                    println(p)
//                }
//                case _ => {
//                    println("结束")
//                }
//            }
//        }
        val obj = oips.readObject()
        if(obj.isInstanceOf[Array[Person]]) {
            val persons = obj.asInstanceOf[Array[Person]]
            for(person <- persons) {
                println(person)
            }
        }
        oips
    }
    def writeObj2File(file:String): Unit = {
        val oops = new ObjectOutputStream(new FileOutputStream(file))
//        oops.writeObject(new Person)
//        oops.writeObject(new Person("小增增", 36))
//        oops.writeObject(new Person("杨洋", 28))

        val arr = Array[Person](new Person, new Person("小增增", 36), new Person("杨洋", 28))
        oops.writeObject(arr)
        oops.close()
    }
}

/**
  * java.io.NotSerializableException:
  * transient关键字不会参与序列化
  */
class Person extends Serializable {
    @transient var name:String = "张三"
    @volatile var age:Int = 19
    def this(name:String, age:Int) {
        this()
        this.name = name
        this.age = age
    }
    override def toString: String = this.name + "--->" + this.age
}
