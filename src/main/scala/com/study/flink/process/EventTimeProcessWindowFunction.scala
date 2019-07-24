package com.study.flink.process

import com.study.flink.stream.EventTimeWordCount.Event
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class EventTimeProcessWindowFunction extends ProcessWindowFunction[Event, (String, String, Int), String, TimeWindow]{

    private val map: mutable.Map[String, Int] = mutable.Map()

    override def process(key: String, context: Context, elements: Iterable[Event],
                         out: Collector[(String, String, Int)]): Unit = {
        val windowStartTime = context.window.getStart
        for (item <- elements) {
            if (map.contains(item.eventType)) {
                map += (item.eventType -> (map.apply(item.eventType) + 1))
            } else {
                map += (item.eventType -> 1)
            }
        }
        for (item <- map) {
            out.collect((FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(windowStartTime), item._1, item._2))
        }
        map.clear()
    }
}
