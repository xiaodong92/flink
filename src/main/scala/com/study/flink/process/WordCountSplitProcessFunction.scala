package com.study.flink.process

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class WordCountSplitProcessFunction extends ProcessFunction[String, (String, Int)] {

    override def processElement(value: String, context: ProcessFunction[String, (String, Int)]#Context,
                                collector: Collector[(String, Int)]): Unit = {
        for (item <- value.split(" ")) {
            collector.collect((item, 1))
        }
    }
}
