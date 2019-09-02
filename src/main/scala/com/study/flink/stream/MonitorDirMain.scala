package com.study.flink.stream

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.{ContinuousFileMonitoringFunction, ContinuousFileReaderOperator, FileProcessingMode}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object MonitorDirMain {

    def main(args: Array[String]): Unit = {
        val path = "file:///Users/lixiaodong/temp/"

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        val format = new TextInputFormat(new Path(path))
        val monitor = new ContinuousFileMonitoringFunction(
            format,
            FileProcessingMode.PROCESS_CONTINUOUSLY,
            1,
            1000
        )
        val reader = new ContinuousFileReaderOperator(format)
        env.addSource(monitor)
            .transform("FileSplitReader", reader)
            .print()

        env.execute("MonitorDirMain")
    }

}
