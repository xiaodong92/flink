package com.study.flink.stream

import com.study.flink.utils.FlinkUtil
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

object KafkaWordCount {

    def main(args: Array[String]): Unit = {
        val params = ParameterTool.fromArgs(args)
        //        val consumerTopic = params.getRequired("consumerTopic")
        //        val consumerServers = params.getRequired("consumerServers")
        //        val consumerGroupId = params.getRequired("consumerGroupId")
        val consumerTopic = "test"
        val consumerServers = "10.224.17.160:9092"
        val consumerGroupId = "test"
        val stateBackendPath = "file:///Users/lixiaodong/temp/checkpoint/kafka_word_count"

        val env = FlinkUtil.getEnv(params, stateBackendPath = stateBackendPath, parallelism = 1)
        val consumer = new FlinkKafkaConsumer010(
            consumerTopic,
            new SimpleStringSchema(),
            FlinkUtil.getKafkaConsumerProp(consumerServers, consumerGroupId)
        )
        val data: DataStream[String] = env.addSource(consumer)
        data.flatMap(_.split(" ")).map((_, 1)).keyBy(0)
            .timeWindow(Time.seconds(5))
            .reduce(new KafkaWordCountReduce, new KafkaWordCountWindowFunction)
            .print().setParallelism(1)
        env.execute("KafkaWordCount")
    }

    class KafkaWordCountReduce extends ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
            (value1._1, value1._2 + value2._2)
        }
    }

    class KafkaWordCountWindowFunction
        extends ProcessWindowFunction[(String, Int), (Long, String, Int), Tuple, TimeWindow] {

        var wordCount: MapState[String, Int] = _

        override def open(parameters: Configuration): Unit = {
            wordCount = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int](
                "wordCount",
                TypeInformation.of(classOf[String]),
                TypeInformation.of(classOf[Int]))
            )
        }

        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)],
                             out: Collector[(Long, String, Int)]): Unit = {
            val element = elements.iterator.next()
            if (wordCount.contains(element._1)) {
                wordCount.put(element._1, element._2 + wordCount.get(element._1))
            } else {
                wordCount.put(element._1, element._2)
            }
            out.collect(context.window.getStart, element._1, wordCount.get(element._1))
        }
    }

}
