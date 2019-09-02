package com.study.flink.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object JoinExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val stream1 = env.readTextFile("/Users/lixiaodong/temp/two_stream_join/a.txt")
            .map(
                msg => {
                    val arr = msg.split(" ")
                    (arr(0), arr(1).toLong * 1000)
                }
            )
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(1)) {
                override def extractTimestamp(element: (String, Long)): Long = {
                    element._2
                }
            })

        val stream2 = env.readTextFile("/Users/lixiaodong/temp/two_stream_join/b.txt")
            .map(
                msg => {
                    val arr = msg.split(" ")
                    (arr(0), arr(1).toLong * 1000)
                }
            )
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(1)) {
                override def extractTimestamp(element: (String, Long)): Long = {
                    element._2
                }
            })

        stream1
            .join(stream2)
            .where(_._1)
            .equalTo(_._1)
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))
            .apply((item1, item2) => {println(item1 + "-" + item2)})

        env.execute("JoinExample")
    }

}
