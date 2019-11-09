This experiment is a simple data generation followed by map operation.

- Single source keeps generating data
- Multiple nodes process data with map and calculates latency.
- Source warms up the execution graph by sending a few tuples then waits for few
millseconds. That gives time for the RDMA network connections to establish connections.

Run job with parallelism of 2 (source parllelism is fixed with 1) and results to be saved to /tmp/map-latency

```bash
flink run -p 2 -c com.rdma.benchmarks.experiment1.StreamingJob rdma-flink-benchmarks-1.0.jar /tmp/map-latency
```

get 10 samples from the output

```bash
shuf -n 10 /tmp/map-latency/<Partition-ID>
```