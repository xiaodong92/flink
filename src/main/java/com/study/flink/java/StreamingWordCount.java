package com.study.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("localhost", 9998)
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    for (String item : value.split(" ")) {
                        out.collect(new Tuple2<>(item, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        dataStream.print().setParallelism(1);

        env.execute("StreamingWordCount");
    }
}
