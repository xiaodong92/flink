package com.study.flink.process

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class WordCountAggProcessFunction(interval: Long) extends KeyedProcessFunction[String, (String, Int), (String, Int)] {

    private var wordCount : Map[String, Int] = Map()
    private var currentMills: Long = 0

    override def processElement(value: (String, Int),
                                ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#Context,
                                out: Collector[(String, Int)]): Unit = {
        if (wordCount.contains(value._1)) {
            wordCount.updated(value._1, wordCount(value._1) + value._2)
        }
        if (0 == currentMills) {
            currentMills = ctx.timerService().currentProcessingTime()
        }
        ctx.timerService().registerEventTimeTimer(currentMills + interval)
    }


    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#OnTimerContext,
                         out: Collector[(String, Int)]): Unit = {

    }

    override def close(): Unit = super.close()
}
