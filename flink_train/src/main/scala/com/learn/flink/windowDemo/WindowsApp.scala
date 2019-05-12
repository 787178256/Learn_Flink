package com.learn.flink.windowDemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Created by kimvra on 2019-05-12
  */
object WindowsApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream("localhost", 9999)

    dataStream.flatMap(_.toLowerCase().split(","))
      .map((_, 1)).keyBy(0)
      //.timeWindow(Time.seconds(5))
      .timeWindow(Time.seconds(10), Time.seconds(5)) // 滑动窗口
      .sum(1).print().setParallelism(1)


    env.execute("WindowApp")
  }

}
