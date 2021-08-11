package dave.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object WordCountFromBatch {

  def main(args: Array[String]): Unit ={

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements("Who's there?",

      "I think I hear them. Stand, ho! Who's there?")

    val counts = text.flatMap(_.toLowerCase().split("\\W+")filter(_.nonEmpty))
      .map((_,1)).groupBy(0).sum(1)

    counts.print()

    /*val env = ExecutionEnvironment.getExecutionEnvironment/*StreamExecutionEnvironment.getExecutionEnvironment*/
    env.setParallelism(1)

    val stream = env.fromElements("xiaomi", "redmi", "oppo", "realme", "vivo", "oppo realme", "realme x7 pro").setParallelism(1)
    stream.flatMap((r) => r.split("\\s") )
    stream.print()*/

//    var textStream = stream.flatMap(((r) => ((r).split("\\s")))).setParallelism(2).map(w => WordWithCount(w, 1)).setParallelism(2).keyBy(0)
//      .sum(1).setParallelism(2)
//    textStream.print().setParallelism(2)
//    env.execute()


  }

  case class WordWithCount(value: Nothing, i: Int)
}
