package com.learn.flink_java.demo1;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;

/**
 * Created by kimvra on 2019-05-09
 */
public class DataSetDataSourceApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        fromCollection(env);
    }

    private static void fromCollection(ExecutionEnvironment env) throws Exception{
        env.fromCollection(Arrays.asList(1,2,3)).print();

    }
}
