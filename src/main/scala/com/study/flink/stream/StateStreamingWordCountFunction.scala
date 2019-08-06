package com.study.flink.stream

import org.apache.flink.api.common.functions.{ReduceFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * -s指定从某一个点回放
  * flink run -s file:///Users/lixiaodong/temp/checkpoint/9807de90981912deaa737d91cdf20b10/chk-282/_metadata \
  * -c com.study.flink.stream.StateStreamingWordCountFunction \
  * target/flink-1.1-SNAPSHOT.jar
  */
object StateStreamingWordCountFunction {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.enableCheckpointing(200)
        env.getCheckpointConfig
            .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.setStateBackend(new FsStateBackend("file:///Users/lixiaodong/temp/checkpoint"))
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

        env.socketTextStream("localhost", 9998).flatMap(_.split(" ")).map((_, 1)).keyBy(0).timeWindow(Time.seconds(5))
            .reduce(new WordCountReduceWindowFunction(), new WordCountProcessWindowFunction)
            .print().setParallelism(1)

        env.execute("StateStreamingWordCountFunction")
    }

    class WordCountReduceWindowFunction extends ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
            (value1._1, value1._2 + value2._2)
        }
    }

    class WordCountProcessWindowFunction extends ProcessWindowFunction[(String, Int), (Long, String, Int),
        Tuple, TimeWindow] {

        private var sum: MapState[String, Int] = _

        override def open(parameters: Configuration): Unit = {
            sum = getRuntimeContext.getMapState(
                new MapStateDescriptor[String, Int](
                    "sum",
                    TypeInformation.of(classOf[String]),
                    TypeInformation.of(classOf[Int]))
            )
        }

        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)],
                             out: Collector[(Long, String, Int)]): Unit = {
            val element = elements.iterator.next()
            if (sum.contains(element._1)) {
                sum.put(element._1, element._2 + sum.get(element._1))
            } else {
                sum.put(element._1, element._2)
            }
            out.collect(context.window.getStart, element._1, sum.get(element._1))
        }
    }
}
