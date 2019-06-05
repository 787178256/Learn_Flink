package com.learn.flink.streamDemo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.types.Row;

/**
 * Created by kimvra on 2019-05-10
 */
public class CustomParallelSourceJava implements ParallelSourceFunction<Tuple2<Boolean, Row>> {

    private boolean isRunning = true;

    private Tuple2<Boolean, Row> data = new Tuple2<>(true, new Row(2));

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            data.f1.setField(0, "k");
            data.f1.setField(1, "women");
            ctx.collect(data);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
