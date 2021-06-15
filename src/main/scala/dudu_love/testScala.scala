package dudu_love

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
object testScala {

  def main(args: Array[String]): Unit = {
    println("love dudu")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    println(env.getJavaEnv)
    env.setParallelism(1)

    //准备数据,类型DataStreamSource
    /*val dataStreamSource = env.fromElements(Tuple2.apply("111","flink")
      ,Tuple2.apply("112","spark")
      ,Tuple2.apply("113","hadoop"))
      .map("i like "+_._2)

    dataStreamSource.flatMap((t1,out:Collector[Tuple2[String,Long]]) => {
      t1.split(" ").foreach(s => out.collect(Tuple2.apply(s,1L)))
    }).print()*/

    env.fromElements(Tuple1.apply("flink jobmanger taskmanager")
      ,Tuple1.apply("spark hadoop")
      ,Tuple1.apply("hadoop hdfs"))
      .flatMap((t1,out:Collector[Tuple2[String,Long]]) => {
        t1._1.split(" ").foreach(s => out.collect(Tuple2.apply(s,1L)))
      })
      .keyBy(0)
      .reduce((t1,t2) => Tuple2.apply(t1._1,t1._2+t2._2))
      .print()

    env.execute("flink map operator")


  }

//  case class WaterSensor(id: String, ts: Long, vc: Double)

}
