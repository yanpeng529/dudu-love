package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LineSplitMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
        String[] items = s.split(" ");

        for (String item: items) {
            collector.collect(new Tuple2<>(item, 1));
        }
    }
}


