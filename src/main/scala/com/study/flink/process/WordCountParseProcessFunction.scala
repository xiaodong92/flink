package com.study.flink.process

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class WordCountParseProcessFunction extends ProcessFunction[String, (String, Int)] {

    override def processElement(value: String, ctx: ProcessFunction[String, (String, Int)]#Context,
                                out: Collector[(String, Int)]): Unit = {
        for (item <- value.split(" ", -1)) {
            out.collect((item, 1))
        }
    }
}
