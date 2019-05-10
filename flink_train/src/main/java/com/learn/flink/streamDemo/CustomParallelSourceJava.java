package com.learn.flink.streamDemo;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * Created by kimvra on 2019-05-10
 */
public class CustomParallelSourceJava implements ParallelSourceFunction {

    private boolean isRunning = true;

    private Long count = 1L;

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
