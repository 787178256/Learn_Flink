package com.learn.flink.demo2

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
/**
  * Flink计数器开发步骤：定义计数器、注册计数器、获取计数器
  * Created by kimvra on 2019-05-09
  */
object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("hadoop","spark","flink","storm")

    val dataSet = data.map(new RichMapFunction[String, Long] {
      // 定义计数器
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // 注册计数器
        getRuntimeContext.addAccumulator("scala-counter", counter)
      }
      override def map(value: String): Long = {
        counter.add(1)
        counter.getLocalValue
      }
    })

    val path = "file:///Users/kimvra/IdeaProjects/imooc/data/counter-sink-out"
    dataSet.writeAsText(path, writeMode = WriteMode.OVERWRITE).setParallelism(2)

    val jobResult = env.execute("CounterJob")
    // 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("scala-counter")

    print("num:" + num)

  }
}
