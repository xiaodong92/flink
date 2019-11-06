package com.study.flink.stream

import com.study.flink.source.CheckpointedSource
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object CheckpointedSourceMain {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)
        env.enableCheckpointing(1000)
        env.addSource(new CheckpointedSource)
            .map(item => (item, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(1))
            .sum(1)
            .print()
        env.execute("CheckpointedSourceMain")
    }

}
