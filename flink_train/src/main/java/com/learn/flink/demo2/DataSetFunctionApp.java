package com.learn.flink.demo2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by kimvra on 2019-05-09
 */
public class DataSetFunctionApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = Arrays.asList(1,2,3,4,5);
        DataSource<Integer> data = env.fromCollection(list);
        //mapFunction(data);
        //filterFunction(data);
        //mapPartitionFunction(env);
        //firstNFunction(env);
        //flatMapFunction(env);
//        distinctFunction(env);
//        joinFunction(env);
        crossFunction(env);
    }

    private static void crossFunction(ExecutionEnvironment env) throws Exception{
        List<String> info1 = Arrays.asList("kimvra", "kok");
        List<Integer> info2 = Arrays.asList(1,2,3);
        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }

    private static void joinFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "kimvra"));
        info1.add(new Tuple2<>(2, "kok"));
        info1.add(new Tuple2<>(3, "simba"));
        info1.add(new Tuple2<>(4, "h"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "广州"));
        info2.add(new Tuple2<>(5, "深圳"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).projectFirst(0, 1).projectSecond(1).print();
    }

    private static void distinctFunction(ExecutionEnvironment env) throws Exception{
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("hadoop,mapreduce");
        list.add("flink,flink");
        DataSource<String> data = env.fromCollection(list);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] tokens = value.split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(token);
                    }
                }
            }
        }).distinct().print();
    }

    private static void flatMapFunction(ExecutionEnvironment env) throws Exception{
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("hadoop,mapreduce");
        list.add("flink,flink");
        DataSource<String> data = env.fromCollection(list);
        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.split(",");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
    }

    private static void firstNFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "Hadoop"));
        list.add(new Tuple2<>(1, "Spark"));
        list.add(new Tuple2<>(1, "MapReduce"));
        list.add(new Tuple2<>(2, "Java"));
        list.add(new Tuple2<>(2, "Spring"));
        list.add(new Tuple2<>(2, "Mysql"));
        list.add(new Tuple2<>(3, "Linux"));
        list.add(new Tuple2<>(3, "Flink"));
        DataSource<Tuple2<Integer, String>> data = env.fromCollection(list);
        data.first(3).print();
        data.groupBy(0).first(2).print();
        data.groupBy(0).first(2).sortPartition(1, Order.ASCENDING).print();

    }

    private static void mapPartitionFunction(ExecutionEnvironment env) throws Exception{
        List<String> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add("student: " + i);
        }
        DataSource<String> data = env.fromCollection(list);
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> input, Collector<String> out) throws Exception {
                System.out.println("connecting");
            }
        }).print();
    }

    private static void mapFunction(DataSource<Integer> data) throws Exception{
        data.map((Integer value) -> value + 1).print();
    }

    private static void filterFunction(DataSource<Integer> data) throws Exception{
        data.map((Integer value) -> value + 1).filter((Integer value) -> value < 4).print();
    }
}
