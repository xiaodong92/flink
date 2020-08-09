package com.study.flink.stream

import java.util.Random
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

object TestKeyedProcessFunction {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.addSource(new RichSourceFunction[(Int, String)] {
            override def run(ctx: SourceFunction.SourceContext[(Int, String)]): Unit = {
                val random = new Random(10)
                while (true) {
                    val t = random.nextInt(10)
                    ctx.collect(t, t.toString)
                    TimeUnit.SECONDS.sleep(2)
                }
            }

            override def cancel(): Unit = {}
        })
            .keyBy(_._1)
            .process(new KeyedProcessFunction[Int, (Int, String), String] {
                @transient private var flagState: MapState[Int, Int] = _
                private var registerTimerFlag = false

                override def open(parameters: Configuration): Unit = {
                    val flagDescriptor = new MapStateDescriptor("flag", TypeInformation.of(classOf[Int]),
                        TypeInformation.of(classOf[Int]))
                    flagState = getRuntimeContext.getMapState(flagDescriptor)
                    println("start open ..")
                }

                override def close(): Unit = {
                    println("start close")
                }

                override def processElement(value: (Int, String),
                                            ctx: KeyedProcessFunction[Int, (Int, String), String]#Context,
                                            out: Collector[String]): Unit = {
                    println(s"before-- value = $value, key = ${ctx.getCurrentKey}, state = ${mapStatToString(flagState)}")
                    if (flagState.contains(value._1)) {
                        flagState.put(value._1, flagState.get(value._1) + 1)
                    } else {
                        flagState.put(value._1, 1)
                    }
                    println(s"after -- value = $value, key = ${ctx.getCurrentKey}, state = ${mapStatToString(flagState)}")
                    out.collect(value._2)
                    if (!registerTimerFlag) {
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000)
                        registerTimerFlag = true
                    }
                }

                override def onTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[Int, (Int, String), String]#OnTimerContext,
                                     out: Collector[String]): Unit = {
                    println(s"on timer start -- key = ${ctx.getCurrentKey}, state = ${mapStatToString(flagState)}")
                    flagState.clear()
                    println(s"on timer end -- key = ${ctx.getCurrentKey}, state = ${mapStatToString(flagState)}")
                    ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000)
                }
            }).print()

        env.execute("test")
    }

    private def mapStatToString(stat: MapState[Int, Int]): String = {
        val builder = new StringBuilder
        for (item <- stat.entries()) {
            builder.append(s"{key = ${item.getKey}, value = ${item.getValue}}, ")
        }
        builder.toString()
    }

}
