package com.study.flink.stream

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindowWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data: DataStream[String] = env.socketTextStream("localhost", 9998)
        val result = data.flatMap(_.split(" ")).map((_, 1)).keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum(1)
        result.print().setParallelism(1)
        env.execute("StreamingWordCount")
    }

}
