package com.learn.flink.demo2

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by kimvra on 2019-05-10
  */
object BroadCastApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val broadCastData = env.fromElements(1,2,3,4,5)
    val data = env.fromElements("hadoop", "spark")

    data.map(new RichMapFunction[String, String] {

      override def open(parameters: Configuration): Unit = {
        val elements = getRuntimeContext.getBroadcastVariable[Int]("broadCastData")
        import scala.collection.JavaConverters._
        for (ele <- elements.asScala) {
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).withBroadcastSet(broadCastData, "broadCastData").print()
  }

}
