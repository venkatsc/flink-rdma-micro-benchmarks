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

package com.rdma.benchmarks.experiment1;


import org.apache.flink.api.common.functions.MapFunction;
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

    public static void main(String[] args) throws Exception {
        System.out.println("usage example flink run -p 2 -c com.rdma.benchmarks.experiment1.StreamingJob " +
                "rdma-flink-benchmarks-1.0.jar /tmp/map-latency\n");
        String outPath = args[0];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Long, Long>> elements = env.addSource(new RandomLongSource());
        DataStream<Tuple2<Long, Long>> sums = elements.map(new MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple2) throws Exception {
                return new Tuple2<>(tuple2.f0, (System.currentTimeMillis() - tuple2.f1));
            }
        });

        sums.writeAsText(outPath,FileSystem.WriteMode.OVERWRITE);
//        sums.addSink(new DiscardingSink<>());
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
