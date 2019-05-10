package com.learn.flink.streamDemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by kimvra on 2019-05-10
 */
public class DataStreamSourceJavaApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        parallelSourceFunction(env);
        nonParallelSourceFunction(env);
        env.execute("StreamJob");
    }

    private static void nonParallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource dataStreamSource = env.addSource(new CustomNonParallelSourceFunction());
        dataStreamSource.print();
    }

    private static void parallelSourceFunction(StreamExecutionEnvironment env) {
        DataStreamSource dataStreamSource = env.addSource(new CustomParallelSourceJava()).setParallelism(2);
        dataStreamSource.print();
    }

    private static void streamFunction(StreamExecutionEnvironment env) {
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).keyBy(1).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);

    }
}
