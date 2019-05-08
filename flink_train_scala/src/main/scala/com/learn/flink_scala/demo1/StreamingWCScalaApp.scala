package com.learn.flink_scala.demo1

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by kimvra on 2019-05-07
  */
object StreamingWCScalaApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var ip = ""
    var port = 0
    try {
      val parameterTool = ParameterTool.fromArgs(args)
      ip = parameterTool.get("ip")
      port = parameterTool.getInt("port")
    } catch {
      case exception: Exception => exception.printStackTrace()
    }
    val text = env.socketTextStream(ip, port)
    import org.apache.flink.api.scala._
    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1)
        .print()
        .setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }
}
