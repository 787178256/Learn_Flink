package com.learn.flink.streamDemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Created by kimvra on 2019-05-10
 */
public class MySQLUtil {

    private static final String url = "jdbc:mysql://localhost:3306/learn_flink?useSSL=false";

    private static final String username = "root";

    private static final String pwd = "admin";

    public static Connection getConnection() {

        Connection connection = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, username, pwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void release(Connection connection, PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
