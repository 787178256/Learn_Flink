package com.learn.flink_java.demo1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * Created by kimvra on 2019-05-07
 */
public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception {
        String input = "/Users/kimvra/IdeaProjects/imooc/data/hello.txt";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile(input);
        // text.print();
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = value.split("\t");
                for (String token : tokens) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }).groupBy(0).sum(1).print();
    }
}
