package com.study.flink.sql

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

object SqlExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tableEnv = StreamTableEnvironment.create(env)
        val data = env.socketTextStream("localhost", 9998)
            .flatMap(_.split(" "))
            .map((_, 1))
        tableEnv.registerDataStream("table_a", data)
        val result = tableEnv.sqlQuery("select _1, sum(_2) from table_a group by _1")
        result.toRetractStream[(String, Int)].print()
        env.execute("SqlExample")
    }
    private case class WordCount(word: String, count: Long)
}
