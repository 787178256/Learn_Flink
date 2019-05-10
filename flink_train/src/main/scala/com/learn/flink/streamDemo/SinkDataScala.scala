package com.learn.flink.streamDemo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._


/**
  * Created by kimvra on 2019-05-10
  */
object SinkDataScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("localhost", 9999)

    val data = dataStream.map(x => {
      val splits: Array[String] = x.split(",")
      StudentScala(splits(0).toInt, splits(1), splits(2).toInt)
    })
    data.addSink(new CustomSinkFunction)
    env.execute("SinJob")
  }
}
