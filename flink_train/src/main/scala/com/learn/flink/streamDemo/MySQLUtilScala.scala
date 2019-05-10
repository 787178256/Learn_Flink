package com.learn.flink.streamDemo

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Created by kimvra on 2019-05-10
  */
object MySQLUtilScala {
  def getConnection() = {
    var connection: Connection = null
    val url = "jdbc:mysql://localhost:3306/learn_flink?useSSl=false"
    val userName = "root"
    val pwd = "admin"
    try {
      connection = DriverManager.getConnection(url, userName, pwd)
    } catch {
      case exception: Exception => {
        exception.printStackTrace()
      }
    }
    connection
  }

  def release(connection: Connection, preparedStatement: PreparedStatement) = {
    try {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
    } catch {
      case exception: Exception => exception.printStackTrace()
    } finally {
      try {
        if (connection != null) {
          connection.close()
        }
      } catch {
        case exception: Exception => exception.printStackTrace()
      }
    }

  }
}
