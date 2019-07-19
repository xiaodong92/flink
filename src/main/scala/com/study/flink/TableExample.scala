package com.study.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.{datastream, environment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.{DefinedProctimeAttribute, StreamTableSource}
import org.apache.flink.types.Row

object TableExample {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tableEnv = StreamTableEnvironment.create(env)
        val data = env.socketTextStream("localhost", 9998)
                .flatMap(_.split(" ")).map(WordCount(_, 1))
        tableEnv.registerDataStream("table_a", data)
        tableEnv.scan("table_a").window(Tumble over 10.minutes on 'UserActionTime as 'userActionWindow)
            .groupBy("word")
            .select("word, count.count as count")
            .toRetractStream[WordCount]
            .print()

        env.execute("TableExample")
    }

    private case class WordCount(word: String, count: Long)

}

class TableExampleSource extends StreamTableSource[Row] with DefinedProctimeAttribute {
    override def getDataStream(execEnv: environment.StreamExecutionEnvironment): datastream.DataStream[Row] = {

    }

    override def getProctimeAttribute: String = ???

    override def getReturnType: TypeInformation[Row] = ???

    override def getTableSchema: TableSchema = ???
}