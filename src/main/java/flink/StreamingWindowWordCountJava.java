package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Author: Mr.Deng
 * Date: 2018/10/15StreamingWindowWordCountJava
 * Desc: 使用flink对指定窗口内的数据进行实时统计，最终把结果打印出来
 *       先在node21机器上执行nc -l 9000
 */
public class StreamingWindowWordCountJava {
    public static String HOST = "127.0.0.1";
    public static Integer PORT = 8823;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(HOST, PORT);

        stream.flatMap(new LineSplitMapFunction()).
                keyBy(0).sum(1).print();


        env.execute("Flink word-count example");
    }
}

