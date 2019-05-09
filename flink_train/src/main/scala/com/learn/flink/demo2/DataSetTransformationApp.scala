package com.learn.flink.demo2

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer
/**
  * Created by kimvra on 2019-05-09
  */
object DataSetTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromCollection(List(1,2,3,4,5))
    //mapFunction(data)
    //filterFunction(data)
    //mapPartitionFunction(env)
    //firstNFunction(env)
    //flatMapFunction(env)
    //distinctFunction(env)
//    joinFunction(env)
    crossFunction(env)
  }


  def crossFunction(env: ExecutionEnvironment) = {
    val info1 = List("kimvra", "kok")
    val info2 = List(1,2,3)

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()
  }

  def joinFunction(env: ExecutionEnvironment) = {
    val info1 = new ListBuffer[(Int, String)]
    info1.append((1, "kimvra"))
    info1.append((2, "kok"))
    info1.append((3, "simba"))
    info1.append((4, "h"))

    val info2 = new ListBuffer[(Int, String)]
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "广州"))
    info2.append((5, "深圳"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
  }

  def distinctFunction(env: ExecutionEnvironment) = {
    val listBuffer = new ListBuffer[String]
    listBuffer.append("hadoop,spark")
    listBuffer.append("hadoop,flink")
    listBuffer.append("flink,flink")
    val data = env.fromCollection(listBuffer)
    data.flatMap(_.split(",")).distinct().print()
  }

  def flatMapFunction(env: ExecutionEnvironment) = {
    val listBuffer = new ListBuffer[String]
    listBuffer.append("hadoop,spark")
    listBuffer.append("hadoop,flink")
    listBuffer.append("flink,flink")
    val data = env.fromCollection(listBuffer)
    data.map(_.split(",")).print()
    data.flatMap(_.split(",")).map(x => (x, 1)).groupBy(0).sum(1).print()

  }

  def firstNFunction(env: ExecutionEnvironment) = {
    val listBuffer = new ListBuffer[(Int, String)]
    listBuffer.append((1, "Hadoop"))
    listBuffer.append((1, "spark"))
    listBuffer.append((1, "flink"))
    listBuffer.append((2, "Java"))
    listBuffer.append((2, "spring"))
    listBuffer.append((3, "python"))
    listBuffer.append((3, "vue"))
    listBuffer.append((4, "js"))
    val data = env.fromCollection(listBuffer)
    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
  }

  def mapPartitionFunction(env: ExecutionEnvironment) = {
    val listBuffer = new ListBuffer[String]
    for (i <- 1 to 10) {
      listBuffer.append("student: " + i)
    }
    val data = env.fromCollection(listBuffer).setParallelism(4)
    data.mapPartition(x => {
      println("connecting...")
      x
    }).print()
  }

  def mapFunction(data: DataSet[Int]) = {
    // data.map(x => x + 1).print()
    data.map(_ + 1).print()
  }

  def filterFunction(data: DataSet[Int]) = {
    data.map(_ + 1).filter(_ > 3).print()
  }
}
