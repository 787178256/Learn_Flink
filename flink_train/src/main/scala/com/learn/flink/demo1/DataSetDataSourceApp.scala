package com.learn.flink.demo1

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by kimvra on 2019-05-09
  */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    fromCollection(env)
  }

  def fromCollection(env: ExecutionEnvironment) = {
    val data = 1 to 10
    import org.apache.flink.api.scala._
    env.fromCollection(data).print()
  }
}
