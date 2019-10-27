package com.rdma.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomLongSource implements SourceFunction<Tuple2<Long, Long>> {
    public static int SIZE= 1_000_000;
//    public int SIZE = 100_000_000;
    private boolean run = true;
    @Override
    public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
        while (run) {
            for (long i = 1; i <= SIZE; i++) {
                if (i == SIZE) {
                    sourceContext.collect(new Tuple2<>(i, System.currentTimeMillis()));
                } else {
                    sourceContext.collect(new Tuple2<>(i, i));
                }
            }
//            randoms);
            Thread.sleep(5);
        }
    }

    @Override
    public void cancel() {
        run = false;
    }
}
