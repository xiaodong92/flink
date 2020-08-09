package com.study.flink.stream

import java.util.Random
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object TestState {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.setStateBackend(new FsStateBackend(args(0), true))

        env.addSource(new RichSourceFunction[Int] {
            override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
                val random = new Random()
                while (true) {
                    ctx.collect(random.nextInt(10))
                    TimeUnit.SECONDS.sleep(1)
                }
            }

            override def cancel(): Unit = {

            }
        }).keyBy(_ % 2)
            .process(new KeyedProcessFunction[Int, Int, Int] {

                @transient private var count: ValueState[Long] = _

                override def open(parameters: Configuration): Unit = {
                    count = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", TypeInformation.of(classOf[Long])))
                }

                override def processElement(value: Int,
                                            ctx: KeyedProcessFunction[Int, Int, Int]#Context,
                                            out: Collector[Int]): Unit = {
                    count.update(count.value() + 1)
                    println(s"key = ${ctx.getCurrentKey}, count = ${count.value()}")
                }
            }).addSink(new RichSinkFunction[Int] {})

        env.execute("TestState")
    }

}
