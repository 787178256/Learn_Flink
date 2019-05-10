package com.learn.flink.streamDemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by kimvra on 2019-05-10
 */
public class SinkData {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator studentStream = dataStreamSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] properties = value.split(",");
                Student student = new Student();
                student.setId(Integer.parseInt(properties[0]));
                student.setName(properties[1]);
                student.setAge(Integer.parseInt(properties[2]));

                return student;
            }
        });
        studentStream.addSink(new StudentSinkFunction());

        env.execute("SinkDataJob");
    }
}
