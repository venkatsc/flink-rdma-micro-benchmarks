package com.rdma.benchmarks.experiment2;

public class WindowLatency {
    private int keyCount=0;
    private long id = 0;
    private long latency = 0;

    public void incrementKeyCount(){
     keyCount++;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public void setKeyCount(int keyCount) {
        this.keyCount = keyCount;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getLatency() {
        return latency;
    }

    public void setLatency(long latency) {
        this.latency = latency;
    }


}
