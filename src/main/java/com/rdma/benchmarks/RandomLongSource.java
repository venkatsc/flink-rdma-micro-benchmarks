package com.rdma.benchmarks;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomLongSource implements ParallelSourceFunction<Tuple2<Long, Long>> {
    public static long POISON=-10;
//    public int SIZE = 100_000_000;
    private boolean run = true;
    private int iterations=1;
    private int currentIteration = 1;
    private int numThreads;
    int poisonPillCount = 1;
    private final LinkedBlockingQueue<Tuple2<Long, Long>> producer;
    public RandomLongSource(LinkedBlockingQueue<Tuple2<Long, Long>> producer,int numThreads){
        this.producer = producer;
        this.numThreads = numThreads;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
        // read from the queue, until poision pill from each thread is received.
        // We
        while (poisonPillCount <= numThreads) {
                Tuple2<Long,Long> tuple = producer.take();
                if (tuple.f0 != POISON) {
                    sourceContext.collect(tuple);
                }else{
                    poisonPillCount++;
                }
            }
    }

    @Override
    public void cancel() {
        run = false;
    }
}
