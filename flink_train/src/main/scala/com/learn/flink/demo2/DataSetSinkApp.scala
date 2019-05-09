package com.learn.flink.demo2

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
/**
  * Created by kimvra on 2019-05-09
  */
object DataSetSinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = 1 to 10
    val dataSet = env.fromCollection(data)

    val path = "file:///Users/kimvra/IdeaProjects/imooc/data/sink-out/"
    dataSet.writeAsText(path, WriteMode.OVERWRITE).setParallelism(2)
    env.execute("DataSetSinkJob")
  }
}
