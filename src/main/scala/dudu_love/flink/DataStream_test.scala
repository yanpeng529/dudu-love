package dudu_love.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.ExecutionEnvironment

object DataStream_test {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //    概述:
//      即通过从一个或多个DataStream生成新的DataStream的过程被称为Transformation操作. 在转换过程中,每种操作类型被
//    定义为不同的Operator,Flink程序都能够将多个Transformation组成一个DataFlow的拓扑.
//    1. Map[DataStream->DataStream]
//      调用用户自定义的MapFunction对DataStream[T]数据进行处理,形成新的DataStream[T],其中数据格式可能会发生变化,
//    常用作对数据集内数据的清洗和转换,例如将输入数据集中的每个数值全部加1处理,并且将数据输出到下游数据集
//    2. FlatMap[DataStream->DataStream]
//      该算子主要应用处理输入一个元素产生一个或者多个元素的计算场景,比较常见的是在经典例子WordCount中,将每一行的
//    文本数据切割,生成单词序列
//    3. Filter[DataStream->DataStream]
//      该算子将按照条件对输入数据集进行筛选操作,将符合条件的数据集输出,将不符合条件的数据过滤掉.
//    3.1 // 通过通配符
//    val filter:DataStream[Int] = dataStream.filter{_%2==0}
//    3.2 // 或者指定运算表达式
//    val filter:DataStream[Int] = dataStream.filter{x=> x%2==0}
//    4. KeyBy[DataStream->KeyedStream]
//      该算子根据指定的key将输入的DataStream[T]数据格式转换为KeyedStream[T],也就是在数据集中执行Partition操作,将
//    相同的Key值的数据放置在相同的分区中.
//      例如WordCount-> 将数据集中第一个参数作为Key,对数据集进行KeyBy函数操作,形成根据Id分区的KeyedStream数据集.
//      eg:
    import org.apache.flink.streaming.api.scala._

    val dataStream = env.fromElements(("a",3),("d",4),("c",2),("a",5))

    // 滚动对第二个字段进行reduce相加求和
    dataStream.map(x=>(x._1, 1)).print()
//    dataStream.filter(_._1.equals("a")).print()
//    dataStream.filter(_._2%2==0).print()

    dataStream.groupBy(_._2).reduce{(x1,x2)=>(x1._1,x1._2+x2._2)}.print()

      //指定第一个字段为分区key

//    5. Reduce[KeyedStream->DataStream]
//      该算子和MapReduce中Reduce原理基本一致,主要目的是将输入的KeyedStream通过传入的用户自定义地ReduceFunction
//    滚动地进行数据聚合处理,其中定义ReduceFunction必须满足运算结合律和交换律,
//    eg:
//      对传入的KeyedStream数据集中相同key值的数据独立进行求和运算,得到每个key所对应的求和值.
//    val dataStream = env.fromElements(("a",3),("d",4),("c",2),("a",5))
//    //指定第一个字段为分区key`
//    val keyedStream:KeyedStream[(String,Int),Tuple] = dataStream.keyBy(0)
//    // 滚动对第二个字段进行reduce相加求和
//    val reduceStream = keyedStream.reduce{(x1,x2)=>(x1._1,x1._2+x2._2)
//      6. Aggregations[KeyedStream->DataStream]
//        Aggregations是KeyedDataStream接口提供的聚合算子,根据指定的字段进行聚合操作,滚动地产生一系列数据聚合结果.
//      其实是将Reduce算子中的函数进行了封装,封装的聚合操作有sum、min、minBy、max、maxBy等,这样就不需要用户自己定义
//      Reduce函数.
//        eg:
//        指定数据集中第一个字段作为key,用第二个字段作为累加字段,然后滚动地对第二个字段的数值进行累加并输出
//      //指定第一个字段为分区key
//      val keyedStream:KeyedStream[(Int,Int),Tuple] = dataStream.keyBy(0)
//      // 对对第二个字段进行sum统计
//      val sumStream:DataStream[(Int,Int)] = keyedStream.sum(1)
//      // 输出计算结果
//      sumStream.print()
//      7. Union[DataStream->DataStream]
//        union算子主要是将两个或者多个输入的数据集合并成一个数据集,需要保证两个数据集的格式一致,输出的数据集的格式和
//      输入的数据集格式保持一致,
//      code:
//      //获取flink实时流处理的环境
//      val env = ExecutionEnvironment.getExecutionEnvironment
//      // 创建不同的数据集
//      val dataStream = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("a", 5))
//      val dataStream2 = env.fromElements(("a", 1), ("d", 1), ("c", 1), ("a", 1))
//      // 调用union算子进行不同的数据集合并
//      dataStream.union(dataStream2).print()
//      8. Connect,CoMap,CoFlatMap[DataStream->ConnectedStream->DataStream](只能在Stream才可以用)
//      connect算子主要是为了合并两种或者多种不同数据类型的数据集,合并后会保留原来原来数据集的数据类型.
//        例如:  dataStream1数据集为(String,Int) 元组类型,dataStream2数据集为Int类型,通过connect连接算子将两个不同数据
//      类型的流结合在一起,形成格式为ConnectedStreams的数据集,其内部数据为[(String,Int),Int]的混合数据类型,保留了两个
//      原始数据集的数据类型
//      eg:
//      //获取flink实时流处理的环境
//      val env = StreamExecutionEnvironment.getExecutionEnvironment
//      // 创建不同的数据集
//      val dataStream = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("a", 5))
//      val dataStream2 = env.fromElements(1, 2, 4, 5)
//      // 连接两个DataStream数据集
//      val connectedStream = dataStream.connect(dataStream2)
//      val result = connectedStream.map(
//        //第一个处理函数
//        t1 => {
//          (t1._1, t1._2)
//        },
//        //第二个处理函数
//        t2 => {
//          (t2, 0)
//        })
//      result.print()
//      env.execute("h")
//      注意:Union和Connect区别
//      1. Union之间两个流的类型必须是一样,Connect可以不一样,在之后的coMap中在去调整成为一样的.
//      2. Connect只能操作两个流,Union可以操作多个
//      9. Split 和 select [DataStream -> SplitStream->DataStream]
//        Split算子是将一个DataStream数据集按照条件进行拆分,形成两个数据集的过程,也是Union算子的逆向实现,每个接入的数据
//      都会被路由到一个或者多个输出数据集中,
//      在使用Splict函数中,需要定义split函数中的切分逻辑,通过调用split函数,然后指定条件判断函数,
//      例如: 如下代码所示,将根据第二个字段的奇偶性将数据集标记出来,如果是偶数则标记为even,如果是奇数则标记为odd,然后通过集合将标记返回,最终生成格式SplitStream的数据集
//      code:
//
//      //获取flink实时流处理的环境
//      val env = StreamExecutionEnvironment.getExecutionEnvironment
//      // 导入Flink隐式转换
//      import org.apache.flink.streaming.api.scala._
//      // 创建不同的数据集
//      val dataStream = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("a", 5))
//      val splitedStream = dataStream.split(t => if (t._2 % 2 == 0) Seq("even") else Seq("odd"))
//
//      // Split函数本身只是对输入数据集进行标记,并没有将数据集真正的实现拆分,因此需要借助Select函数根据标记将数据切分成不同的数据集,
//      //筛选出偶数数据集
//      val evenStream = splitedStream.select("even").print()
//      //筛选出偶数数据集
//      val oddStream = splitedStream.select("odd")
//
//      env.execute("l ")
  }
}
