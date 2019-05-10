package com.learn.flink.demo2;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * 步骤：定义计数器、注册计数器、获取计数器
 * Created by kimvra on 2019-05-09
 */
public class CounterJavaApp {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromElements("hadoop", "spark", "storm", "flink");

        DataSet<String> dataSet = dataSource.map(new RichMapFunction<String, String>() {

            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("counter", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String path = "file:///Users/kimvra/IdeaProjects/imooc/data/counter-java/";
        dataSet.writeAsText(path, FileSystem.WriteMode.OVERWRITE).setParallelism(2);

        JobExecutionResult result = env.execute("CounterJavaJob");
        Long count = result.getAccumulatorResult("counter");
        System.out.println("counter:" + count);
    }
}
