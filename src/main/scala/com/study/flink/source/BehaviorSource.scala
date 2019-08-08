package com.study.flink.source

import com.study.flink.common.Constants
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.mutable

class BehaviorSource extends SourceFunction[String] {

    @volatile private var isRunning = true
    private val events = Array("exposure", "exposure", "click", "exposure", "click")

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (isRunning) {
            for (_ <- 0 to 100) {
                val data = getData
                for (item <- data) {
                    println(s"**********source collect -> $item")
                    ctx.collect(item)
                }
            }
            isRunning = false
        }
    }

    private def getData: Array[String] = {
        val arr: mutable.ArrayBuffer[String] = mutable.ArrayBuffer()
        for (event <- events) {
            arr += s"$event|${FastDateFormat.getInstance(Constants.defaultTimeFormat).format(System.currentTimeMillis())}"
            Thread.sleep(1000)
        }
        arr += s"click|${FastDateFormat.getInstance(Constants.defaultTimeFormat).format(System.currentTimeMillis() - 3000)}"
        arr += s"click|${FastDateFormat.getInstance(Constants.defaultTimeFormat).format(System.currentTimeMillis() - 5000)}"
        arr += s"exposure|${FastDateFormat.getInstance(Constants.defaultTimeFormat).format(System.currentTimeMillis() - 1000)}"
        arr.toArray
    }

    override def cancel(): Unit = isRunning = false
}
