package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author: Mr.Deng
 * Date: 2018/10/19
 * Desc:
 */
public class WordCountJava {
    public static void main(String[] args) throws Exception {
        //构建环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //通过字符串构建数据集
        DataSet<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");
        //分割字符串、按照key进行分组、统计相同的key个数
        DataSet<Tuple2<String, Integer>> wordCounts  = text
                .flatMap(new LineSplitter())
                .groupBy(0)
         .sum(1);

//        tuple2UnsortedGrouping.withPartitioner(sum);
//        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> first = tuple2UnsortedGrouping.first(1);

//打印
        wordCounts.print();
    }
    //分割字符串的方法
    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
