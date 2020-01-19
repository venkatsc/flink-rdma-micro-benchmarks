package com.rdma.benchmarks.experiment2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class WindowLatencyAggregator implements AggregateFunction<Tuple2<Long, Long>, WindowLatency, Tuple3<Long,Long,
        Long>> {

    @Override
    public WindowLatency createAccumulator() {
        return new WindowLatency();
    }

    @Override
    public WindowLatency add(Tuple2<Long, Long> tuple2, WindowLatency windowLatency) {
        // trigger calculations on last window element
        if (tuple2.f0 == StreamingJob.PRODUCER_ELEMENTS_PER_ITERATION && windowLatency.getKeyCount() == StreamingJob.CountWindowSize
                - 1) { // aggregate for only few windows once
            windowLatency.setId(tuple2.f0);
            long triggerTimestamp= System.currentTimeMillis();
            windowLatency.setLatency( triggerTimestamp - tuple2.f1);
            windowLatency.setTriggerTimestamp(triggerTimestamp);
        } else {
            windowLatency.incrementKeyCount();
        }
        return windowLatency;
    }

    @Override
    public Tuple3<Long, Long,Long> getResult(WindowLatency windowLatency) {
        return new Tuple3<>(windowLatency.getId(), windowLatency.getTriggerTimestamp(), windowLatency.getLatency());
    }

    @Override
    public WindowLatency merge(WindowLatency windowLatency, WindowLatency acc1) {
        return null;
    }
}
