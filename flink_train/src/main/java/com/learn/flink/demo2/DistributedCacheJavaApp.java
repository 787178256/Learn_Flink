package com.learn.flink.demo2;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.List;

/**
 * 步骤：注册一个本地/HDFS文件、在open方法中获取分布式缓存的内容
 * Created by kimvra on 2019-05-10
 */
public class DistributedCacheJavaApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "file:///Users/kimvra/IdeaProjects/imooc/data/hello.txt";

        DataSource<String> data = env.fromElements("hadoop", "spark", "storm");
        env.registerCachedFile(filePath, "simba-java-cached");

        data.flatMap(new RichFlatMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("simba-java-cached");
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    System.out.println(line);
                }
            }

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
            }
        }).print();
    }
}
