package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.MarketingUserBehavior;
import dave.flink.ecommerce.beans.MarketingViewCount;
import dave.flink.ecommerce.beans.SimulatedEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<Tuple3<String, String, Long>, Tuple> ds = env.addSource(new SimulatedEventSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MarketingUserBehavior>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(MarketingUserBehavior marketingUserBehavior) {
                        return marketingUserBehavior.getTimestamp();
                    }
                })
                .filter( data -> (!data.getBehavior().equals("UNINSTALL")))
                .map(new MapFunction<MarketingUserBehavior, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(MarketingUserBehavior value) throws Exception {
                        return Tuple3.of(value.getChannel(), value.getBehavior(), 1L);
                    }
                })
                .keyBy(0, 1);
        DataStream<MarketingViewCount>  resultDS = ds.timeWindow(Time.hours(1), Time.seconds(10))
                .process(new MarketingCountByChannel());

//        ds.print();
        resultDS.print();
        env.execute();
    }

    static class MarketingCountByChannel extends ProcessWindowFunction<Tuple3<String, String, Long>, MarketingViewCount, Tuple, TimeWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> iterable, Collector<MarketingViewCount> collector) throws Exception {
            long startTs = context.window().getStart();
            long endTs = context.window().getEnd();
//            System.out.println("=========" + endTs);
            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);

            long count = 0L;
            for (Tuple3<String, String, Long> str: iterable) {
                count++;
            }
            collector.collect(new MarketingViewCount(String.valueOf(startTs), String.valueOf(endTs), channel, behavior, count));
        }
    }
}
