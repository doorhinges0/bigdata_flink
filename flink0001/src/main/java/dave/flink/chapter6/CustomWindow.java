package dave.flink.chapter6;

import dave.flink.util.SensorReading;
import dave.flink.util.SensorSource;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.util.Collection;
import java.util.Collections;

public class CustomWindow {

    public static void main(String[]  args) throws  Exception {

        /*StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointInterval(10_1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading>  sensorReadingDataStream = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());*/
        String hostName = "192.168.74.91";
        int port = 8765;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorReadingDataStream = env.socketTextStream(hostName, port, "\n").map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {

                String[] ss = value.split("\\'");
                String[] equals = value.split("\\=");
                String s_id = ss[1];
                String s_timestamp  = equals[2].split("\\,")[0].substring(0,equals[2].split("\\,")[0].length()-1);
                String[] ss2= equals[3].split("\\,");
                String s_temperature = ss2[0].substring(0,ss2[0].length()-1);
                return new SensorReading(s_id, Long.valueOf(s_timestamp), Double.valueOf(s_temperature));
            }
        }).assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple4<String, Long, Long, Integer>>  countsPerThirtySecs = sensorReadingDataStream
                .keyBy(r -> r.id)
                .window(new ThirtySecondWindows())
                .trigger(new OneSecondIntervalTrigger())
                .process(new CountFunction());

        countsPerThirtySecs.print();
        env.execute();
    }


    public static class ThirtySecondWindows extends WindowAssigner<Object, TimeWindow> {

        long windowSize = 30_1000L;

        @Override
        public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {

            // rounding down by 30 seconds
            long startTime = timestamp - (timestamp % windowSize);
            long endTime = startTime - windowSize;
            return Collections.singletonList(new TimeWindow(startTime, endTime));
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return EventTimeTrigger.create();
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }
    }



    public static class OneSecondIntervalTrigger  extends Trigger<SensorReading, TimeWindow> {

        @Override
        public TriggerResult onElement(SensorReading r, long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            // firstSeen will be false if not set yet
            ValueState<Boolean>  firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));

            // register initial timer only for first element
            if (firstSeen.value() == null) {
                //compute time for next early firing by rounding watermark to second
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentProcessingTime() % 1000));
                ctx.registerEventTimeTimer(t);
                //register timer for the end of the window
                ctx.registerEventTimeTimer(w.getEnd());
                firstSeen.update(true);
            }
            // continue. Do not evaluate per element
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // Continue. We don't use processing time timers
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long ts, TimeWindow w, TriggerContext ctx) throws Exception {
            if (ts == w.getEnd()) {
                // final evaluation and purge window state
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                // register next early firing timer
                long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
                if (t < w.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                // fire trigger to early evaluate window
                return TriggerResult.FIRE;
            }
        }

        @Override
        public void clear(TimeWindow w, TriggerContext ctx) throws Exception {
            // clear trigger state
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
            firstSeen.clear();
        }
    }

    public static class CountFunction extends ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Integer>, String, TimeWindow> {

        @Override
        public void process(String id, Context ctx, java.lang.Iterable<SensorReading> readings, Collector<Tuple4<String, Long, Long, Integer>> out) throws Exception {
            //count readings
            int cnt = 0;
            for (SensorReading r : readings) {
                cnt++;
            }
            //get current watermark
            long evalTime = ctx.currentWatermark();
            //emit result
            out.collect(Tuple4.of(id, ctx.window().getEnd(), evalTime, cnt));
        }
    }

}
