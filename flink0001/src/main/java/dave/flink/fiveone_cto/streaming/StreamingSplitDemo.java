package dave.flink.fiveone_cto.streaming;

import dave.flink.fiveone_cto.custormsource.MyNoParalleSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamingSplitDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Long> d2s = env.addSource(new MyNoParalleSource());

        SplitStream<Long>  splitStream = d2s.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> output = new ArrayList<String>();
                if (value % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        });

        DataStream<Long> even =  splitStream.select("even");
        even.shuffle();
        even.rebalance();
        DataStream<Long> odd =  splitStream.select( "odd");
        odd.print("odd===").setParallelism(2);
        even.print("even===").setParallelism(2);
        env.execute();

    }
}
