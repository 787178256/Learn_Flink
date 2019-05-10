package com.learn.flink.streamDemo

import org.apache.flink.streaming.api.functions.sink.SinkFunction

/**
  * Created by kimvra on 2019-05-10
  */
class CustomSinkFunction extends SinkFunction[StudentScala]{

  val sql = "insert into student(id,name,age) values(?,?,?)"


  override def invoke(student: StudentScala, context: SinkFunction.Context[_]): Unit = {
    val connection = MySQLUtilScala.getConnection()
    connection.setAutoCommit(false)
    val preparedStatement = connection.prepareStatement(sql)

    preparedStatement.setInt(1, student.id)
    preparedStatement.setString(2, student.name)
    preparedStatement.setInt(3, student.age)

    preparedStatement.execute()
    connection.commit()

    MySQLUtilScala.release(connection, preparedStatement)

  }
}
