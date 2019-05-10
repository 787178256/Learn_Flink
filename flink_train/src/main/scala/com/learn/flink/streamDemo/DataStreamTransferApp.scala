package com.learn.flink.streamDemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * Created by kimvra on 2019-05-10
  */
object DataStreamTransferApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    filterFunction(env)
    unionFunction(env)
    env.execute("DataStreamJob")
  }

  def filterFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomParallelSourceFunction)
    data.map(x => {
      println("received: " + x)
      x
    }).filter(_ % 2 == 0).print().setParallelism(1)
  }

  def unionFunction(env: StreamExecutionEnvironment) = {
    val data1 = env.addSource(new CustomParallelSourceFunction)
    val data2 = env.addSource(new CustomParallelSourceFunction)

    data1.union(data1).print().setParallelism(1)
  }

  def splitFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomParallelSourceFunction)
    data.split((num: Long) => {
      num % 2 match {
        case 0 => List("even")
        case 1 => List("odd")
      }
    }).select("even").print().setParallelism(1)
  }

}
