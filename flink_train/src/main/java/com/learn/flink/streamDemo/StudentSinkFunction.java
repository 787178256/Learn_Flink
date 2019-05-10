package com.learn.flink.streamDemo;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created by kimvra on 2019-05-10
 */
public class StudentSinkFunction implements SinkFunction<Student> {

    private static final String sql = "insert into student(id,name,age) values (?,?,?)";

    @Override
    public void invoke(Student student, Context context) throws Exception {

        Connection connection = MySQLUtil.getConnection();
        connection.setAutoCommit(false);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        preparedStatement.setInt(1, student.getId());
        preparedStatement.setString(2, student.getName());
        preparedStatement.setInt(3, student.getAge());

        preparedStatement.execute();
        connection.commit();

        MySQLUtil.release(connection, preparedStatement);
    }
}
