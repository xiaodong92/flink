package com.study.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * word count
  * 使用 nc -lk 9998 写测试数据
  */
object StreamingWordCount {

    def main(args: Array[String]): Unit = {
        /**
          * 创建执行环境
          * 如果程序是独立调用的，返回本地执行环境
          * 如果是客户端命令行调用提交到集群，返回此集群的执行环境
          */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val data: DataStream[String] = env.socketTextStream("localhost", 9998)
        val result = data.flatMap(_.split(" ")).map((_, 1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1)
        result.print().setParallelism(1)
        env.execute("StreamingWordCount")
    }

}
