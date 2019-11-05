package com.rdma.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomLongSource implements SourceFunction<Tuple2<Long, Long>> {
    public static int SIZE= 10_000_000;
//    public int SIZE = 100_000_000;
    private boolean run = true;
    private int iterations=2;
    private int currentIteration =1;
    @Override
    public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
        Thread.sleep(3000);
        while (run) {
            for (long i = 1; i <= SIZE; i++) {
                sourceContext.collect(new Tuple2<>(i, System.currentTimeMillis()));
            }

            if (currentIteration==iterations){
                run=false;
            }else{
                currentIteration++;
            }
//            randoms);
//            Thread.sleep(5);
        }
    }

    @Override
    public void cancel() {
        run = false;
    }
}
