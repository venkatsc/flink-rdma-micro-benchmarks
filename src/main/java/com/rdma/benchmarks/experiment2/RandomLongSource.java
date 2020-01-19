package com.rdma.benchmarks.experiment2;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class RandomLongSource implements ParallelSourceFunction<Tuple2<Long, Long>> {
    public static long POISON = -10;
    //    public int SIZE = 100_000_000;
    private boolean run = true;
    private int iterations = 1;
    private int currentIteration = 1;
    private int producerThreads;
    int poisonPillCount = 0;
    private final ArrayBlockingQueue<Tuple2<Long, Long>> producer = new ArrayBlockingQueue<>(10_000_000);

    public RandomLongSource(int producerThreads) {
        this.producerThreads = producerThreads;
    }

    @Override
    public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {

        // ATTENTION: Flink only establishes the network connections on first data element.
        // So, our experiments should send some data before taking metrics. That means, our experiments would
        // become complicated.
        // Reference: https://cwiki.apache.org/confluence/display/FLINK/Data+exchange+between+tasks
        for (int j = 0; j < StreamingJob.CountWindowSize; j++) {
            for (long i = -1; i > -10_0; i--) {
                sourceContext.collect(new Tuple2<>(i, System.currentTimeMillis()));
            }
        }
        // let connection setup and warmup data finish
        Thread.sleep(30000);

        // start producer threads
//        for (int i = 0; i < producerThreads; i++) {
//            new Thread(new Producer(producer)).start();
//        }
        // read from the queue, until poision pill from each thread is received on the current instance of source.
        // If two threads are generating data on the current source instance, then two poision pills should be received
        // on the source.
        int iter = 0;
        while (iter < StreamingJob.PRODUCER_NUMBER_OF_ITERATIONS) {
            iter++;
                for (long i = 1; i <= StreamingJob.PRODUCER_ELEMENTS_PER_ITERATION; i++) {
                    if (i == StreamingJob.PRODUCER_ELEMENTS_PER_ITERATION) { // time stamp last element in the generated size
                        sourceContext.collect(new Tuple2<>(i, System.currentTimeMillis()));
                    } else {
                        sourceContext.collect(new Tuple2<>(i, -i));
                    }
                }

        }

//        while (poisonPillCount < producerThreads) {
//            Tuple2<Long, Long> tuple = producer.take();
//            if (tuple.f0 != POISON) {
//                sourceContext.collect(tuple);
//            } else {
//                System.out.println("Received poision pill " + poisonPillCount);
//                poisonPillCount++;
//            }
//        }
    }

    @Override
    public void cancel() {
        run = false;
    }
}


class Producer implements Runnable {
    RateLimiter rateLimiter = RateLimiter.create(StreamingJob.PRODUCER_RATE_LIMIT);

    //    static int SIZE = 1000;
    BlockingQueue<Tuple2<Long, Long>> producer;
//    public static int maxIterations = 500;

    public Producer(BlockingQueue<Tuple2<Long, Long>> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        int iter = 0;
        while (iter < StreamingJob.PRODUCER_NUMBER_OF_ITERATIONS) {
            iter++;
            rateLimiter.acquire(StreamingJob.PRODUCER_ELEMENTS_PER_ITERATION);
            try {
                for (long i = 1; i <= StreamingJob.PRODUCER_ELEMENTS_PER_ITERATION; i++) {
                    if (i == StreamingJob.PRODUCER_ELEMENTS_PER_ITERATION) { // time stamp last element in the generated size
                        producer.put(new Tuple2<>(i, System.currentTimeMillis()));
                    } else {
                        producer.put(new Tuple2<>(i, -i));
                    }
                }
                Thread.yield();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        // add poision pill from this thread
        // Should only add one poision pill per thread as data source depends on the pill count.
        producer.add(new Tuple2<>(RandomLongSource.POISON, System.currentTimeMillis()));
    }
}
