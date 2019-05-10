package com.learn.flink.streamDemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Created by kimvra on 2019-05-10
  */
object DataStreamSourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    socketFunction(env)
//    nonParallelSourceFunction(env)
    parallelSourceFunction(env)
    env.execute("DataStreamJob")
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    data.print()

  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }

  def socketFunction(env: StreamExecutionEnvironment) = {
    val dataStream = env.socketTextStream("localhost", 9999)
    dataStream.flatMap(_.split(",")).map((_, 1))
      .keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1)
  }
}
