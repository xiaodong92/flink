package com.study.flink.stream

import com.study.flink.process.EventTimeProcessWindowFunction
import com.study.flink.source.BehaviorSource
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ProcessTimeWordCount {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.addSource(new BehaviorSource)
            .map(item => {
                val arr = item.split("\\|")
                Event(arr(0), FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").parse(arr(1)).getTime)
            })
            .keyBy(_.eventType)
            .timeWindow(Time.seconds(2))
            .process(new EventTimeProcessWindowFunction())
            .print().setParallelism(1)

        env.execute("ProcessTimeWordCount")
    }

}
