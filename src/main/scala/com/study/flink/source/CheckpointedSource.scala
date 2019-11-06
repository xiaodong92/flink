package com.study.flink.source

import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.state.{CheckpointListener, FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class CheckpointedSource extends RichParallelSourceFunction[String] with CheckpointListener with CheckpointedFunction {

    private val arr = Array("a", "b")

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
        println(s"-- ${getRuntimeContext.getIndexOfThisSubtask} ==> notifyCheckpointComplete")
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
        println(s"-- ${getRuntimeContext.getIndexOfThisSubtask} ==> snapshotState")
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
        println(s"-- ${getRuntimeContext.getIndexOfThisSubtask} ==> initializeState")
    }

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        import scala.util.control.Breaks._
        var count = 0
        breakable {
            while (true) {
                count += 1
                ctx.collect(arr(getRuntimeContext.getIndexOfThisSubtask)
                )
                TimeUnit.MICROSECONDS.sleep(100)
                if (count > 10000) {
                    break()
                }
            }
        }

    }

    override def cancel(): Unit = {
        println("close")
    }
}
