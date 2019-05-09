package com.learn.flink.demo1

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by kimvra on 2019-05-09
  */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //fromCollection(env)
    //textFile(env)
    csvFile(env)
  }

  def csvFile(env: ExecutionEnvironment) = {
    val path = "file:///Users/kimvra/IdeaProjects/imooc/data/sales.csv"
    import org.apache.flink.api.scala._
    env.readCsvFile[(Int, Int, Int, Double)](path, ignoreFirstLine = true).print()
    env.readCsvFile[(Int, Int)](path, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
    env.readCsvFile[Person](path, ignoreFirstLine = true).print()
    env.readCsvFile[Sale](path, ignoreFirstLine = true, pojoFields = Array("transactionId", "customerId", "itemId", "amountPaid")).print()
  }
  def textFile(env: ExecutionEnvironment) = {
    val path = "file:///Users/kimvra/IdeaProjects/imooc/data/hello.txt"
    env.readTextFile(path).print()
  }

  def fromCollection(env: ExecutionEnvironment) = {
    val data = 1 to 10
    import org.apache.flink.api.scala._
    env.fromCollection(data).print()
  }
}
case class Person(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

