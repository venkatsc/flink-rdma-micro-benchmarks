package com.rdma.benchmarks.experiment2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class WindowLatencyAggregator implements AggregateFunction<Tuple2<Long, Long>, WindowLatency, Tuple2<Long,Long>> {

    @Override
    public WindowLatency createAccumulator() {
        return new WindowLatency();
    }

    @Override
    public WindowLatency add(Tuple2<Long, Long> tuple2, WindowLatency windowLatency) {
        // trigger calculations on last window element
        if (windowLatency.getKeyCount() == StreamingJob.CountWindowSize - 1){
            windowLatency.setId(tuple2.f0);
            windowLatency.setLatency(System.currentTimeMillis() - tuple2.f1);
        }else{
            windowLatency.incrementKeyCount();
        }
        return windowLatency;
    }

    @Override
    public Tuple2<Long, Long> getResult(WindowLatency windowLatency) {
        return new Tuple2<>(windowLatency.getId(),windowLatency.getLatency());
    }

    @Override
    public WindowLatency merge(WindowLatency windowLatency, WindowLatency acc1) {
        return null;
    }
}
