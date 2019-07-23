package com.study.flink.process

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class EventTimeProcessWindowFunction extends ProcessWindowFunction[(String, Int), (String, String, Int), Tuple, TimeWindow]{

    private val map: mutable.Map[String, Int] = mutable.Map()

    override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)],
                         out: Collector[(String, String, Int)]): Unit = {
        val windowStartTime = context.window.getStart
        for (item <- elements) {
            if (map.contains(item._1)) {
                map += (item._1 -> (map.apply(item._1) + item._2))
            } else {
                map += (item._1 -> item._2)
            }
        }
        for (item <- map) {
            out.collect((FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(windowStartTime), item._1, item._2))
        }
        map.clear()
    }
}
