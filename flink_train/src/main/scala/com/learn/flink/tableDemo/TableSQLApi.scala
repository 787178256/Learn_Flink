package com.learn.flink.tableDemo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * Created by kimvra on 2019-05-12
  */
object TableSQLApi {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val path = "file:///Users/kimvra/IdeaProjects/imooc/data/sales.csv"
    val csv = env.readCsvFile[SalesLog](path, ignoreFirstLine = true)
    csv.print()
    val salesTable = tableEnv.fromDataSet(csv)
    tableEnv.registerTable("sales", salesTable)
    val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")
    tableEnv.toDataSet[Row](resultTable).print()

  }
}

case class SalesLog(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)
