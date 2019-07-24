package com.study.flink.stream

import com.study.flink.common.Constants
import com.study.flink.java.BoundedOutOfOrdernessTimestampExtractor
import com.study.flink.process.EventTimeProcessWindowFunction
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import com.study.flink.source.BehaviorSource

object EventTimeWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val stream = env.addSource(new BehaviorSource)
        val withTimestampsAndWatermarks = stream
            .map(m => {
                val arr = m.split("\\|")
                Event(arr(0), FastDateFormat.getInstance(Constants.defaultTimeFormat).parse(arr(1)).getTime)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Event](Time.seconds(1)) {
                override def extractTimestamp(element: Event): Long = {
                    element.timestamp
                }
            })
        withTimestampsAndWatermarks
            .keyBy(_.eventType)
            .timeWindow(Time.seconds(2))
            .process(new EventTimeProcessWindowFunction())
            .print()

        env.execute("EventTimeWordCount")
    }

    case class Event(eventType: String, timestamp: Long)
}
