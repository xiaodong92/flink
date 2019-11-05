package com.study.flink.stream

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object KeyBySortMain {

    private val arr = Array("a", "b", "c")

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(5)
        env.addSource(new RichParallelSourceFunction[(String, Int, Long)] {
            override def run(ctx: SourceFunction.SourceContext[(String, Int, Long)]): Unit = {
                for (item <- 0 to 20) {
                    if (getRuntimeContext.getIndexOfThisSubtask < arr.length) {
                        val key = arr(getRuntimeContext.getIndexOfThisSubtask)
                        val outValue = (key, item, System.currentTimeMillis())
                        println(s"${getRuntimeContext.getIndexOfThisSubtask}> source outValue = $outValue")
                        TimeUnit.MILLISECONDS.sleep(1)
                        ctx.collect(outValue)
                    }
                }
            }

            override def cancel(): Unit = {

            }
        }).map(item => item).setParallelism(10).keyBy(_._1).print()

        env.execute("test")
    }

}
