package com.learn.flink.demo2;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kimvra on 2019-05-09
 */
public class DataSetSinkJavaApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        DataSource<Integer> dataSource = env.fromCollection(data);
        dataSource.print();

        String path = "file:///Users/kimvra/IdeaProjects/imooc/data/sink-out-java/";
        dataSource.writeAsText(path, FileSystem.WriteMode.OVERWRITE);
        env.execute("DataSetSinkJob");
    }
}
