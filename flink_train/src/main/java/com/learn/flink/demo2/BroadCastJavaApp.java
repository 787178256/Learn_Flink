package com.learn.flink.demo2;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * Created by kimvra on 2019-05-10
 */
public class BroadCastJavaApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> toBroadCast = env.fromElements(1,2,3,4,5);
        DataSource<String> data = env.fromElements("hadoop", "spark", "storm");
        
        data.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                List<Integer> brodCastJava = getRuntimeContext().getBroadcastVariable("BrodCastJava");
                for (Integer i : brodCastJava) {
                    System.out.println(i);
                }

            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).withBroadcastSet(toBroadCast, "BrodCastJava").print();

    }
}
