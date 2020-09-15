package com.study.flink.stream

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object OperatorStateTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
        env.addSource(new RichSourceFunction[Long] {
            var flag = true

            override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
                while (flag) {
                    ctx.collect(System.currentTimeMillis())
                    TimeUnit.MILLISECONDS.sleep(100)
                }
            }

            override def cancel(): Unit = {
                flag = false
            }
        })
            .keyBy(_ % 2)
            .process(new KeyedProcessFunction[Long, Long, (Long, Int)] with CheckpointedFunction {
                @transient var checkpointedState: ListState[Long] = _
                val cache = new ListBuffer[Long]
                var timerFlag = false

                override def processElement(value: Long,
                                            ctx: KeyedProcessFunction[Long, Long, (Long, Int)]#Context,
                                            out: Collector[(Long, Int)]): Unit = {
                    cache.add(value)
                    if (!timerFlag) {
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000)
                        timerFlag = true
                    }
                }

                override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Long, (Long, Int)]#OnTimerContext, out: Collector[(Long, Int)]): Unit = {
                    out.collect(ctx.timerService().currentProcessingTime(), cache.size)
                    cache.clear()
//                    timerFlag = false
                    ctx.timerService().registerProcessingTimeTimer(timestamp + 2000)
                }

                override def snapshotState(context: FunctionSnapshotContext): Unit = {
//                    println(checkpointedState.get())
                    checkpointedState.clear()
                    checkpointedState.addAll(cache)
//                    println(checkpointedState.get())
                }

                override def initializeState(context: FunctionInitializationContext): Unit = {
                    val descriptor = new ListStateDescriptor[Long]("test-cache", classOf[Long])
                    checkpointedState = context.getOperatorStateStore.getListState(descriptor)
                    println(checkpointedState.get())
                    if (context.isRestored) {
                        for (item <- checkpointedState.get()) {
                            cache += item
                        }
                    }
                }
            }).print()

        env.execute("test")
    }

}
