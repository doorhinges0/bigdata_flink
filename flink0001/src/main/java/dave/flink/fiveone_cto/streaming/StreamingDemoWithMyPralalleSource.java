package dave.flink.fiveone_cto.streaming;

import dave.flink.fiveone_cto.custormsource.MyRichParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDemoWithMyPralalleSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> d2s = env.addSource(new MyRichParalleSource()/*new MyParalleSource()*/);
        SingleOutputStreamOperator<Long> ds = d2s.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value + 1;
            }
        })
                .filter(new FilterFunction<Long>() {
                    @Override
                    public boolean filter(Long aLong) throws Exception {
                        return aLong % 2 == 0;
                    }
                });

        ds.timeWindowAll(Time.seconds(2)).sum(0).print("sum==");
        ds.print();

        env.execute();

    }
}
