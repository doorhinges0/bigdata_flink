package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.ItemViewCount;
import dave.flink.ecommerce.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Collections;


public class HotItems {

    public static void main(String[] args) {

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(3);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStream<UserBehavior>  ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\UserBehavior.csv")
                    .map(new MapFunction<String, UserBehavior>() {
                        @Override
                        public UserBehavior map(String value) throws Exception {
                            String[] array = value.split(",");
                            return new UserBehavior(Long.valueOf(array[0].trim()), Long.valueOf(array[1].trim()), Integer.valueOf(array[2].trim()), array[3].trim(), Integer.valueOf(array[4].trim()));
                        }
                    })
                    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.minutes(5)) {
                        @Override
                        public long extractTimestamp(UserBehavior userBehavior) {
                            return userBehavior.getTimestamp() * 1000L;
                        }
                    });

//            DataStream<UserBehavior> filterDS =  ds.filter(data -> (data.getBehavior().equals("pv")));

            DataStream<ItemViewCount> procesStream =  ds.filter(data -> (data.getBehavior().equals("pv")))
                    .keyBy(data -> data.getItemId())
                    .timeWindow(Time.hours(1), Time.minutes(5))
                    .aggregate(new CountAgg(), new WindowResult());

            DataStream<String>  resultDS = procesStream.keyBy(data -> data.getWindowEnd())
                    .process(new TopNHotItems(3));

            resultDS.print();
            env.execute("hot item job");
        }catch (Exception exception) {
            System.out.println(exception);
        }

    }

    static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String>{

        private int topSize;
        private ListState<ItemViewCount> itemViewCountListState;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-state", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {

            itemViewCountListState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<ItemViewCount> allItems = new ArrayList<ItemViewCount>();
            for (ItemViewCount ivc:itemViewCountListState.get()) {
                allItems.add(ivc);
            }
//            allItems.sort(new IVCComparator());
            Collections.sort(allItems, new IVCComparator());
            int allItemsLength = allItems.size() - 1;

//            System.out.println("================size" + allItems.size());
            itemViewCountListState.clear();
            String result = new String();
            result += "time:" + new Timestamp(timestamp -1).toString() + "\n";
            /*for (int i = 0; i < allItemsLength; i++) {
                if (allItems.get(i).getItemId() == 2338453)
                    System.out.println("================size=" + i + ",time:" + new Timestamp(timestamp -1).toString() + allItems.get(i).toString());
                if (allItems.get(i).getItemId() == 812879)
                    System.out.println("================size=" + i + ",time:" + new Timestamp(timestamp -1).toString() + allItems.get(i).toString());
                if (allItems.get(i).getItemId() == 2563440)
                    System.out.println("================size=" + i + ",time:" + new Timestamp(timestamp -1).toString() + allItems.get(i).toString());
            }*/
            for (int i = 0; i < topSize; i++) {
                ItemViewCount ivc = allItems.get(allItemsLength - i);
                result += "no:" + (i +1) + ":";
                result += " productId=" + ivc.getItemId();
                result += " pageview=" + ivc.getCount();
                result += "\n";
            }
            result += "====================";
            Thread.sleep(1000);
            out.collect(result);
        }

    }

    public static class IVCComparator implements  Comparator<ItemViewCount> {

        @Override
        public int compare(ItemViewCount o1, ItemViewCount o2) {

            return o1.getCount() < o2.getCount() ? -1 :(o1.getCount() > o2.getCount() ? 1 : 0);
        }
    }

    static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    static class WindowResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {


        @Override
        public void apply(Long key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {

            collector.collect(new ItemViewCount(key, timeWindow.getEnd(), iterable.iterator().next()));

        }
    }

    static class AverageAgg implements AggregateFunction<UserBehavior, Tuple2<Long, Integer>, Double> {

        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return new Tuple2<>(0L, 0);
        }

        @Override
        public Tuple2<Long, Integer> add(UserBehavior value, Tuple2<Long, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Long, Integer> accumulator) {
            return Double.valueOf(accumulator.f0 / accumulator.f1);
        }

        @Override
        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

}


