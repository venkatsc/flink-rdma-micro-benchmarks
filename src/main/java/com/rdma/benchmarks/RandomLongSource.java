package com.rdma.benchmarks;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class RandomLongSource implements ParallelSourceFunction<Tuple2<Long, Long>> {
    public static long POISON=-10;
//    public int SIZE = 100_000_000;
    private boolean run = true;
    private int iterations=1;
    private int currentIteration = 1;
    private int producerThreads;
    int poisonPillCount = 1;
    private final LinkedBlockingQueue<Tuple2<Long, Long>> producer =  new LinkedBlockingQueue<>();
    public RandomLongSource(int producerThreads){
        this.producerThreads = producerThreads;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
        // read from the queue, until poision pill from each thread is received.
        Thread.sleep(3000); // RDMA connection setup, should not be included in latency calculations.

        // start producer threads
        for (int i = 0; i < producerThreads; i++) {
            new Thread(new Producer(producer)).start();
        }

        while (poisonPillCount <= producerThreads) {
                Tuple2<Long,Long> tuple = producer.take();
                if (tuple.f0 != POISON) {
                    sourceContext.collect(tuple);
                }else{
                    System.out.println("Received poision pill "+poisonPillCount);
                    poisonPillCount++;
                }
            }
    }

    @Override
    public void cancel() {
        run = false;
    }
}


class Producer implements Runnable {
    static int SIZE = 1_000;
    //    static int SIZE = 1000;
    LinkedBlockingQueue<Tuple2<Long, Long>> producer;
    int maxIterations = 5_000;

    public Producer(LinkedBlockingQueue<Tuple2<Long, Long>> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        int iter = 0;
        while (iter < maxIterations) {
            iter++;
            for (long i = 0; i < SIZE; i++) {
                producer.add(new Tuple2<>(i, System.currentTimeMillis()));
            }
        }
        // add poision pill from this thread
        // Should only add one poision pill per thread as data source depends on the pill count.
        producer.add(new Tuple2<>(RandomLongSource.POISON, System.currentTimeMillis()));
    }
}