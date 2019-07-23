package source

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.io.{BufferedSource, Source}

class BehaviorSource extends SourceFunction[String] {

    @volatile var isRunning = true

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val data = getData
        while (isRunning) {
            for (item <- data) {
                ctx.collect(item)
            }
            isRunning = false
        }
    }

    private def getData: Array[String] = {
        val source: BufferedSource = Source
            .fromFile(this.getClass.getResource("/").getPath + "../../src/main/resources/event_time.txt")
        val lines = source.getLines().toArray
        source.close()
        lines
    }

    override def cancel(): Unit = isRunning = false
}
