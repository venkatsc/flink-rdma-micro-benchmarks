/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rdma.benchmarks.experiment2;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {
//    static LinkedBlockingQueue<Tuple2<Long, Long>> producer = new LinkedBlockingQueue<>();
    public static int CountWindowSize = 5000;
    public static int PRODUCER_ELEMENTS_PER_ITERATION = 1_000;
    public static int PRODUCER_NUMBER_OF_PER_ITERATION = 500_000;
    public static int PRODUCER_RATE_LIMIT = 500_000;
    public static int SOURCE_PARALLELISM = 3;


    public static void main(String[] args) throws Exception {
        System.out.println("usage rdma-*.jar <outpath> <producerThreadCount>");

        System.out.println("\n\nOutput size would be ");

        String outPath = args[0];
        int producerThreads = Integer.parseInt(args[1]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // we may want to vary this
        env.setBufferTimeout(-1);
        DataStream<Tuple2<Long, Long>> elements = env.addSource(new RandomLongSource(producerThreads)).setParallelism(SOURCE_PARALLELISM);
        DataStream<Tuple2<Long, Long>> avgLatency = elements.keyBy(0).
                       countWindow(CountWindowSize).aggregate(new WindowLatencyAggregator());

        // output size should be threadCount* Producer.maxIterations* Producer.SIZE/CountWindowSize
        avgLatency.writeAsCsv(outPath, FileSystem.WriteMode.OVERWRITE);
        env.execute("Flink Streaming Java API Skeleton");
    }


//    private static class AverageLatencyWindowFunction implements AggregateFunction<Tuple2<Long, Long>, Tuple2<Long, Double>, Tuple, GlobalWindow> {
//
//
//        @Override
//        public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple2<Long, Long>> values,
//                          Collector<Tuple2<Long, Double>> collector) throws Exception {
//            double sum=0;
//            long currentMills = System.currentTimeMillis();
//            for (Tuple2<Long,Long> value: values) {
//                sum+= currentMills - value.f1;
//            }
//
//            long key=0;
//
//            for (Tuple2<Long,Long> value: values
//                 ) {
//                key = value.f0;
//                break;
//            }
//
//            collector.collect(new Tuple2<>(key,sum/CountWindowSize));
//        }
//    }
}
