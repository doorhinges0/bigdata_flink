package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.collection.immutable.Stream;

public class PageView {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        DataStream<Tuple2<String, Integer>> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] array = value.split(",");
                        return new UserBehavior(Long.valueOf(array[0].trim()), Long.valueOf(array[1].trim()), Integer.valueOf(array[2].trim()), array[3].trim(), Integer.valueOf(array[4].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(5)) {

                    @Override
                    public long extractTimestamp(UserBehavior s) {
                        return s.getTimestamp() * 1000L;
                    }
                })
                .filter( data -> data.getBehavior().equals("pv"))
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                        return new Tuple2<String, Integer>("pv", 1);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .sum(1);

        ds.print();
        env.execute();
    }
}
