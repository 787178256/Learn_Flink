package com.learn.flink.demo2

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by kimvra on 2019-05-10
  */
object DistributeCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "file:///Users/kimvra/IdeaProjects/imooc/data/hello.txt"
    env.registerCachedFile(filePath, "simba-scala")

    val data = env.fromElements("hadoop", "spark", "storm")
    data.map(new RichMapFunction[String, String]() {

      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext.getDistributedCache.getFile("simba-scala")
        val lines = FileUtils.readLines(file)
        import scala.collection.JavaConverters._
        for (ele <- lines.asScala) {
          println(ele)
        }

      }
      override def map(value: String): String = {
        value
      }
    }).print()
  }
}
