package com.study.flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object BatchWordCount {

    def main(args: Array[String]): Unit = {
        val path = "/etc/hosts"
        val env = ExecutionEnvironment.getExecutionEnvironment
        val dataSet: DataSet[String] = env.readTextFile(path)
        dataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1).print()
    }

}
