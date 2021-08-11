package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.UserBehavior;
import dave.flink.ecommerce.beans.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.HashSet;
import java.util.Set;

public class UniqueVisitor {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        DataStream<UvCount> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] array = value.split(",");
                        return new UserBehavior(Long.valueOf(array[0].trim()), Long.valueOf(array[1].trim()), Integer.valueOf(array[2].trim()), array[3].trim(), Integer.valueOf(array[4].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(1)) {

                    @Override
                    public long extractTimestamp(UserBehavior s) {
                        return s.getTimestamp() * 1000L;
                    }
                })
                .filter( data -> data.getBehavior().equals("pv"))
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountByWindow());

        ds.print();
        env.execute();

    }

    static class UvCountByWindow implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> input, Collector<UvCount> out) {

            Set<Long> userSet = new HashSet<>();

            for (UserBehavior ub: input) {
//                System.out.println("===========" + ub);
                userSet.add(ub.getUserId());
            }
            out.collect(new UvCount(window.getEnd(), Long.valueOf(userSet.size())));
        }
    }
}
