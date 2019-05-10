package com.learn.flink.streamDemo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by kimvra on 2019-05-10
 */
public class CustomNonParallelSourceJava implements SourceFunction<Long> {

    private boolean isRunning = true;

    private Long count = 1L;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
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
