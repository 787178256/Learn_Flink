package com.learn.flink_scala.demo2

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * Created by kimvra on 2019-05-08
  */
object StreamingWCScalaApp1 {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 9999
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream(host, port)
    // text.print()
    import org.apache.flink.api.scala._
    text.flatMap(_.toLowerCase.split(","))
      .map(x => WordCount(x, 1))
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")
      .print()
      .setParallelism(1)
    env.execute("StreamingJob")

  }
}

case class WordCount(word: String, count: Int)

