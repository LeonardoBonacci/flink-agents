package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * A minimal Flink DataStream job that runs embedded (no external Flink cluster needed).
 *
 * <p>Reads a bounded stream of sample sentences, tokenizes them into words,
 * and counts occurrences per word — a classic streaming word-count.
 *
 * <p>This serves as the foundation for a Flink Agents POC: the pipeline structure
 * (source → process → sink) maps directly to the agents pattern where an
 * {@code AgentsExecutionEnvironment} wraps the {@code StreamExecutionEnvironment}
 * and agent operators replace simple map/flatMap stages.
 */
public class SimpleDataStreamJob {

    public static void main(String[] args) throws Exception {
        // Set up the embedded streaming execution environment (mini-cluster)
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Sample source — a small bounded stream of sentences
        DataStream<String> source = env.fromData(
                "Apache Flink is a framework for stateful stream processing",
                "Flink Agents extends Flink with agentic AI capabilities",
                "Agents can reason and act on streaming data in real time",
                "This is a simple DataStream example to get started with Flink",
                "We will extend this pipeline with Flink Agents next"
        );

        // Tokenize sentences into (word, 1) tuples and count per word
        DataStream<Tuple2<String, Integer>> wordCounts = source
                .flatMap(new Tokenizer())
                .keyBy(tuple -> tuple.f0)
                .sum(1);

        // Print results to stdout
        wordCounts.print();

        // Execute the pipeline
        env.execute("Simple DataStream Word Count");
    }

    /**
     * Splits each input sentence into lowercase words and emits (word, 1) tuples.
     */
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            for (String word : value.toLowerCase().split("\\W+")) {
                if (!word.isEmpty()) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }
    }
}
