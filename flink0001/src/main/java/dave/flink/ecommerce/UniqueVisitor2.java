package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.UserBehavior;
import dave.flink.ecommerce.beans.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class UniqueVisitor2 {

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

            long count = 0L;

            BloomFilter bloomFilter = BloomFilter.create(Funnels.longFunnel(), 100000);
            for (UserBehavior ub: input) {
//                System.out.println("===========" + ub);
                Long uid = ub.getUserId();
                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid);
                    count++;
                }
            }
            out.collect(new UvCount(window.getEnd(), count));
        }
    }
}
