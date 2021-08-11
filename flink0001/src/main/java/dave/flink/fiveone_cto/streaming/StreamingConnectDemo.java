package dave.flink.fiveone_cto.streaming;

import dave.flink.fiveone_cto.custormsource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class StreamingConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> d2s = env.addSource(new MyNoParalleSource());
        DataStreamSource<Long> d2s2 = env.addSource(new MyNoParalleSource());
        ConnectedStreams<String, Long> connectedStreams = d2s2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return String.valueOf(value) + "_str";
            }
        }).connect(d2s);

        connectedStreams.map(new CoMapFunction<String, Long, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Long value) throws Exception {
                return value;
            }
        }).print();

//        ds.timeWindowAll(Time.seconds(2)).sum(0).print("sum==");
//        ds.print();

        env.execute();

    }
}
