package com.study.flink.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object ProcessWindowFunctionMain {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val data: DataStream[String] = env.socketTextStream("localhost", 9999)
        data.flatMap(_.split(" ")).map((_, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(2))
            .process(new MyProcessWindowFunction)
            .print()

        env.execute("test")
    }

    class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), String, Tuple, TimeWindow] {

        private val globalMap: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            val currentMap: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()
            for (item <- elements) {
                globalMap.put(item._1, globalMap.getOrElse(item._1, 0) + item._2)
                currentMap.put(item._1, currentMap.getOrElse(item._1, 0) + item._2)
            }

            println(s"current = $currentMap")
            println(s"global = $globalMap")
        }
    }

}
