package com.study.flink.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingWindowWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data: DataStream[String] = env.socketTextStream("localhost", 9998)
        val result = data.flatMap(_.split(" ")).map((_, 1)).keyBy(0)
            .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))).sum(1)
        result.print().setParallelism(1)
        env.execute("StreamingWordCount")
    }

}
