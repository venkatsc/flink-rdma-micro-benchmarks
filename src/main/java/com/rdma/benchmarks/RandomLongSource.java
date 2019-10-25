package com.rdma.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class RandomLongSource implements SourceFunction<List<Tuple2<Long,Long>>> {
    private static int n= 10_000_000;
    private boolean run= true;
    private static  ThreadLocalRandom random = ThreadLocalRandom.current();

    private static List<Tuple2<Long,Long>> randoms = new ArrayList<>(n);
    static {
        for (long i=0;i<n;i++) {
            randoms.add(new Tuple2<>(i, i));
        }
    }
    @Override
    public void run(SourceContext<List<Tuple2<Long, Long>>> sourceContext) throws Exception {
        while (run) {
//            randoms.forEach(sourceContext::collect);
//            for (int i=0;i<random.nextInt(5,10);i++) {
                sourceContext.collect(randoms);
//            }
//            Thread.sleep(5);
        }
    }

    @Override
    public void cancel() {
        run=false;
    }
}
