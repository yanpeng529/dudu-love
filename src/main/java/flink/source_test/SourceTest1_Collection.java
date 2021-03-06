package flink.source_test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author : Ashiamd email: ashiamd@foxmail.com
 * @date : 2021/1/31 5:13 PM
 * 测试Flink从集合中获取数据
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置env并行度1，使得整个任务抢占同一个线程执行
        env.setParallelism(1);

        // Source: 从集合Collection中获取数据
        DataStream<SensorReading> dataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        DataStream<Integer> intStream = env.fromElements(1,2,3,4,5,6,7,8,9);
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("/Users/mahaili/IdeaProjects/dudu/src/main/resources/sensor.txt");
        // 打印输出
        dataStream.print();
//        KeyedStream<SensorReading, Tuple> sensorReadingTupleKeyedStream =
        final KeyedStream<SensorReading, Tuple> id = dataStream.keyBy("id");
        id.map((MapFunction<SensorReading, String>) longLongTuple2 -> "111").print();
//        sensorReadingTupleKeyedStream.getKeyType();
        intStream.print();
        stringDataStreamSource.print();
        // 执行
        env.execute("JobName");

    }

}
