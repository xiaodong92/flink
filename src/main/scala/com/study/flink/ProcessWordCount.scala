package com.study.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object ProcessWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data: DataStream[String] = env.socketTextStream("localhost", 9998)
//        data.process()
    }

}
