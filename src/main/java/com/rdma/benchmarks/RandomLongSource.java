package com.rdma.benchmarks;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomLongSource implements SourceFunction<Tuple2<Long, Long>> {
    public static int SIZE= 10_000_000;
//    public int SIZE = 100_000_000;
    private boolean run = true;
    private int iterations=1;
    private int currentIteration = 1;
    private final LinkedBlockingQueue<Tuple2<Long, Long>> producer;
    public RandomLongSource(LinkedBlockingQueue<Tuple2<Long, Long>> producer){
        this.producer = producer;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
        while (run) {
                sourceContext.collect(producer.take());
            }
    }

    @Override
    public void cancel() {
        run = false;
    }
}
