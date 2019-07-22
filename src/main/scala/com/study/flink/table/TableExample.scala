package com.study.flink.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object TableExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tableEnv = StreamTableEnvironment.create(env)
        val data = env.socketTextStream("localhost", 9998)
                .flatMap(_.split(" ")).map(WordCount(_, 1))
        tableEnv.registerDataStream("table_a", data)
        val result = tableEnv.scan("table_a")
            .groupBy("word")
            .select('word, 'count.sum as 'count)

        result.toRetractStream[WordCount].print()
        env.execute("TableExample")
    }
    private case class WordCount(word: String, count: Long)
}
