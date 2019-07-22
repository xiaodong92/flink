package com.study.flink.process

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ProcessWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data: DataStream[String] = env.socketTextStream("localhost", 9998)
        data.process(new WordCountSplitProcessFunction())
            .keyBy(0)
            .process(new WordCountProcessFunction(2000))
            .print().setParallelism(1)

        env.execute("ProcessWordCount")
    }

}
