package com.study.flink.process

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable

class WordCountProcessFunction(interval: Long) extends KeyedProcessFunction[Tuple, (String, Int), (String, String, Int)] {

    private val wordCount : mutable.Map[String, Int] = mutable.Map()
    private var lastMills: Long = 0

    override def processElement(value: (String, Int),
                                ctx: KeyedProcessFunction[Tuple, (String, Int), (String, String, Int)]#Context,
                                out: Collector[(String, String, Int)]): Unit = {
        if (wordCount.contains(value._1)) {
            wordCount += (value._1 -> (wordCount.apply(value._1) + value._2))
        } else {
            wordCount += (value._1 -> value._2)
        }
        if (0 == lastMills) {
            lastMills = ctx.timerService().currentProcessingTime() / interval * interval
        }
        ctx.timerService().registerProcessingTimeTimer(lastMills + interval)
    }


    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Tuple, (String, Int), (String, String, Int)]#OnTimerContext,
                         out: Collector[(String, String, Int)]): Unit = {
        if (timestamp == lastMills + interval) {
            synchronized {
                for (item <- wordCount) {
                    out.collect(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(lastMills), item._1, item._2)
                }
                wordCount.clear()
            }
            lastMills = timestamp
        }
    }

    override def close(): Unit = super.close()
}
