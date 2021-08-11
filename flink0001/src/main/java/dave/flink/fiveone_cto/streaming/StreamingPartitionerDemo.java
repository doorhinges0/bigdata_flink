package dave.flink.fiveone_cto.streaming;

import dave.flink.fiveone_cto.custormsource.MyNoParalleSource;
import dave.flink.fiveone_cto.custormsource.MyPartitioner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamingPartitionerDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);

        DataStreamSource<Long> d2s = env.addSource(new MyNoParalleSource());
        SingleOutputStreamOperator<Tuple1<Long>> mapDS= d2s.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                return new Tuple1<>(value);
            }
        });
        DataStream<Tuple1<Long>> ds = mapDS.partitionCustom(new MyPartitioner(), 0);
        ds.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("thread:" + Thread.currentThread().getId() + "," + value);
                return value.f0;
            }
        }).print("partition=====");
        ds.print("======");
        String path = "E:\\flink_dev\\flink0001\\src\\main\\resources\\output";
        ds.writeAsText(path);
        env.execute();

    }
}
