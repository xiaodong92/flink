package com.study.flink.stream

import com.study.flink.process.EventTimeProcessWindowFunction
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import source.BehaviorSource

object EventTimeWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.addSource(new BehaviorSource)
            .map(m => {
                val arr = m.split("\\|")
                Event(arr(0), FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(arr(1)).getTime)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Event](Time.seconds(1)) {
                override def extractTimestamp(element: Event): Long = {
                    element.timestamp
                }
            })
            .map(item => (item.eventType, 1))
            .keyBy(0)
            .timeWindow(Time.seconds(3))
            .process(new EventTimeProcessWindowFunction())
            .print().setParallelism(1)

        env.execute("EventTimeWordCount")
    }

    private case class Event(eventType: String, timestamp: Long)
}
