package com.learn.flink_scala.demo1


import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by kimvra on 2019-05-07
  */
object BatchWCScalaApp {
  def main(args: Array[String]): Unit = {
    val path = "file:///Users/kimvra/IdeaProjects/imooc/data/hello.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(path)
    // text.print()

    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1).print()
  }
}
