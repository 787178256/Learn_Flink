package com.learn.flink.streamDemo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kimvra on 2019-05-10
 */
public class DataStreamTransferJava {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        filterFunction(env);
        unionFunction(env);
        env.execute("StreamTransferJob");
    }

    private static void splitFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStreamSource = env.addSource(new CustomParallelSourceJava());
        dataStreamSource.split(new OutputSelector<Long>() {
            List<String> list = new ArrayList<>();
            @Override
            public Iterable<String> select(Long value) {
                if (value % 2 == 0) {
                    list.add("even");
                } else {
                    list.add("odd");
                }
                return list;
            }
        }).select("even").print().setParallelism(1);
    }

    private static void filterFunction(StreamExecutionEnvironment env) {
        DataStreamSource dataStreamSource = env.addSource(new CustomParallelSourceJava());
        dataStreamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("received:" + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        }).print().setParallelism(1);
    }

    private static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> dataStreamSource = env.addSource(new CustomParallelSourceJava());
        DataStreamSource<Long> dataStreamSource1 = env.addSource(new CustomParallelSourceJava());

        dataStreamSource.union(dataStreamSource1).print().setParallelism(1);
    }
}
