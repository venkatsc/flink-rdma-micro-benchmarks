This experiment is a simple data generation followed by map operation.

- Deploys multiple parallel sources for generating data. Each source with 'n' threads produces
 the data and writes it to the blocking queue. The flink source reads the data from blocking queue.
- The data is keyed by the id, windowed for every 'k' records and avg latency is calculated. 

Run job with parallelism of 2 (using parallel sources) and results to be saved to /tmp/map-latency. The data generator
 starts with 3 threads per source instance.

```bash
flink run -p 2 -c com.rdma.benchmarks.experiment2.StreamingJob rdma-flink-benchmarks-1.0.jar /tmp/map-latency 3
```

get 10 samples from the output

```bash
shuf -n 10 /tmp/map-latency/<Partition-ID>
```