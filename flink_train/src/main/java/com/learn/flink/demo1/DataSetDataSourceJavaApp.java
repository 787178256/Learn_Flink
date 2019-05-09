package com.learn.flink.demo1;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;

/**
 * Created by kimvra on 2019-05-09
 */
public class DataSetDataSourceJavaApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //fromCollection(env);
        //textFile(env);
        //readCsvFile(env);
        readRecursiveFile(env);
    }

    private static void readRecursiveFile(ExecutionEnvironment env) throws Exception{
        String path = "file:///Users/kimvra/IdeaProjects/imooc/data/nested";
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(path).withParameters(parameters).print();
    }

    private static void readCsvFile(ExecutionEnvironment env) throws Exception{
        String path = "file:///Users/kimvra/IdeaProjects/imooc/data/sales.csv";

        env.readCsvFile(path).ignoreFirstLine().types(Integer.class, Integer.class, Integer.class, Double.class).print();
    }
    private static void textFile(ExecutionEnvironment env) throws Exception{
        String path = "file:///Users/kimvra/IdeaProjects/imooc/data/hello.txt";
        env.readTextFile(path).print();
    }
    private static void fromCollection(ExecutionEnvironment env) throws Exception{
        env.fromCollection(Arrays.asList(1,2,3)).print();
    }
}
