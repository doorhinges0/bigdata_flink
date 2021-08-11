package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.ItemViewCount;
import dave.flink.ecommerce.beans.UserBehavior;
import dave.flink.ecommerce.beans.UvCount;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;


public class UvWithBloom {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        DataStream<UvCount> ds =
        /*DataStream<UserBehavior> ds = */env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\UserBehavior.csv")
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
//                .apply(new UvCountByWindow());
//                .keyBy(data ->data.getBehavior())
//                .timeWindow(Time.hours(1))
//                .trigger(new MyTrigger())
                .process(new UvCountByAllWindow());
//                .aggregate();

//        KeyedStream<UserBehavior, String> keyedStream = ds.keyBy(data -> data.getBehavior());
//        SingleOutputStreamOperator<Long>  distinctStream = keyedStream.map(new MyRichMapFunction());
//        distinctStream.timeWindowAll(Time.hours(1));

                /*.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<String, Long>("dummyKey", value.getUserId());
                    }
                })
                .keyBy(data -> data.f1)
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountWithBloom());*/

//        distinctStream.print();
        ds.print();
        env.execute();

    }

    static class UvCountByAllWindow extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {

//        private transient ValueState<BloomFilter> uidState;
//        private transient ValueState<Long> countState;

        /*@Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
            uidState = getRuntimeContext().getState(stateDescriptor);
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
            countState = getRuntimeContext().getState(countDescriptor);
        }*/

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {

            ValueState<BloomFilter> uidState;
            ValueState<Long> countState;

            ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
            uidState = getRuntimeContext().getState(stateDescriptor);
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
            countState = getRuntimeContext().getState(countDescriptor);

            for (UserBehavior ub: elements) {
//                System.out.println("===========" + ub);
                String uid = String.valueOf(ub.getUserId());
                BloomFilter bloomFilter = uidState.value();
                if (bloomFilter == null) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    countState.update(0L);
                }
                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid);
                    countState.update(countState.value() + 1);
                }
                uidState.update(bloomFilter);
            }
            out.collect(new UvCount(context.window().getEnd(), Long.valueOf(countState.value())));
        }
    }

    /*static class UvCountByWindow implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        ValueState<BloomFilter> uidState;
        ValueState<Long> countState;

        ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
        uidState = getRuntimeContext().getState(stateDescriptor);
        ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
        countState = getRuntimeContext().getState(countDescriptor);

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> input, Collector<UvCount> out) {

            for (UserBehavior ub: iterable) {
//                System.out.println("===========" + ub);
                String uid = String.valueOf(ub.getUserId());
                BloomFilter bloomFilter = uidState.value();
                if (bloomFilter == null) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    countState.update(0L);
                }
                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid);
                    countState.update(countState.value() + 1);
                }
                uidState.update(bloomFilter);
            }

            out.collect(new UvCount(context.window().getEnd(), Long.valueOf(countState.value())));

            Set<Long> userSet = new HashSet<>();

            for (UserBehavior ub: input) {
//                System.out.println("===========" + ub);
                userSet.add(ub.getUserId());
            }
            out.collect(new UvCount(window.getEnd(), Long.valueOf(userSet.size())));
        }
    }*/



//    static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {
//
//    }

//    static class Bloom implements Serializable {
//
//    }

//    static class UvCountWithBloom extends ProcessWindowFunction<Tuple2<String, Long>, UvCount, String, TimeWindow> {
//
//        @Override
//        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UvCount> out) throws Exception {
//
//        }
//    }

    static class UvCountWithBloom extends ProcessWindowFunction<UserBehavior, UvCount, String/*Long*/, TimeWindow> {


//        private transient ValueState<BloomFilter> uidState;
//        private transient ValueState<Long> countState;

//        @Override
//        public void open(Configuration parameters) throws Exception {

//            ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
//            uidState = getRuntimeContext().getState(stateDescriptor);
//            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
//            countState = getRuntimeContext().getState(countDescriptor);
//        }

        public Long map(UserBehavior value) throws Exception {
            ValueState<BloomFilter> uidState;
            ValueState<Long> countState;
            ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
            uidState = getRuntimeContext().getState(stateDescriptor);
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
            countState = getRuntimeContext().getState(countDescriptor);

            String uid = String.valueOf(value.getUserId());
            BloomFilter bloomFilter = uidState.value();
            if (bloomFilter == null) {
                bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                countState.update(0L);
            }
            if (!bloomFilter.mightContain(uid)) {
                bloomFilter.put(uid);
                countState.update(countState.value() + 1);
            }
            uidState.update(bloomFilter);
            return countState.value();
        }

        @Override
        public void process(String/*Long*/ s, Context context, Iterable<UserBehavior> iterable, Collector<UvCount> collector) throws Exception {

            ValueState<BloomFilter> uidState;
            ValueState<Long> countState;

            ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
            uidState = getRuntimeContext().getState(stateDescriptor);
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
            countState = getRuntimeContext().getState(countDescriptor);

            for (UserBehavior ub: iterable) {
//                System.out.println("===========" + ub);
                String uid = String.valueOf(ub.getUserId());
                BloomFilter bloomFilter = uidState.value();
                if (bloomFilter == null) {
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                    countState.update(0L);
                }
                if (!bloomFilter.mightContain(uid)) {
                    bloomFilter.put(uid);
                    countState.update(countState.value() + 1);
                }
                uidState.update(bloomFilter);
            }

            collector.collect(new UvCount(context.window().getEnd(), Long.valueOf(countState.value())));
        }
    }

    static class MyRichMapFunction extends RichMapFunction<UserBehavior, Long> {

        private transient ValueState<BloomFilter> uidState;
        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<BloomFilter>("uid-state", BloomFilter.class);
            uidState = getRuntimeContext().getState(stateDescriptor);
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>("count-state", Long.class);
            countState = getRuntimeContext().getState(countDescriptor);
        }

        @Override
        public Long map(UserBehavior value) throws Exception {

            String uid = String.valueOf(value.getUserId());
            BloomFilter bloomFilter = uidState.value();
            if (bloomFilter == null) {
                bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                countState.update(0L);
            }
            if (!bloomFilter.mightContain(uid)) {
                bloomFilter.put(uid);
                countState.update(countState.value() + 1);
            }
            uidState.update(bloomFilter);
            return countState.value();
        }
    }

    static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior stringLongTuple2, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        }
    }


}
