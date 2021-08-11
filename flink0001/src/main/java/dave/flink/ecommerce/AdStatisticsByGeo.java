package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.AdClickEvent;
import dave.flink.ecommerce.beans.ApacheLogEvent;
import dave.flink.ecommerce.beans.BlackListWarning;
import dave.flink.ecommerce.beans.CountByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.tools.nsc.doc.model.Val;

import java.io.IOException;
import java.sql.Timestamp;

public class AdStatisticsByGeo {

    private static  OutputTag<BlackListWarning> blackListWarningOutputTag = new OutputTag<BlackListWarning>("blacklist"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<AdClickEvent> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\AdClickLog.csv")
                .map(new MapFunction<String, AdClickEvent>() {
                    @Override
                    public AdClickEvent map(String value) throws Exception {
                        String[] line = value.split(",");
                        return new AdClickEvent(Long.valueOf(line[0].trim()), Long.valueOf(line[1]), line[2].trim(), line[3].trim(), Long.valueOf(line[4].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AdClickEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(AdClickEvent adClickEvent) {
                        return adClickEvent.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<AdClickEvent> filterDS = ds.keyBy(new KeySelector<AdClickEvent, Tuple2<Long, String>>() {

            @Override
            public Tuple2<Long, String> getKey(AdClickEvent value) throws Exception {
                return new Tuple2<>(value.getUserId(), value.getProvince());
            }
        })
                .process(new FilterBlackListUser(100));

        DataStream<CountByProvince> dataStream = filterDS.keyBy(data -> data.getProvince())
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AddCountAgg(), new AdCountResult());

        filterDS.getSideOutput(blackListWarningOutputTag).print("blacklist==");
        dataStream.print();
        env.execute();
    }

    static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, String>, AdClickEvent, AdClickEvent> {
        private long maxCount;
        private ValueState<Long> countState;
        private ValueState<Boolean> isSentBlacklist;
        private ValueState<Long> resetTimer;

        public FilterBlackListUser(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            isSentBlacklist = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("issent-state", Boolean.class));
            resetTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("resettime-state", Long.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {
            Long curCount = countState.value();
            Boolean isSentBlack = isSentBlacklist.value();
            if (curCount == null) {
                curCount = 0L;
                long ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24);
                resetTimer.update(ts);
                ctx.timerService().registerEventTimeTimer(ts);
            }
            if (isSentBlack == null) {
                isSentBlack = false;
            }
            if (curCount >= maxCount) {
                if (!isSentBlack){
                    isSentBlacklist.update(true);
                    ctx.output(blackListWarningOutputTag, new BlackListWarning(value.getUserId(), value.getAdId(), "click over " + maxCount + " times today."));
                }
                return;
            }
            countState.update( curCount + 1);
            out.collect(value);
        }
    }
}

class AddCountAgg implements AggregateFunction<AdClickEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(AdClickEvent value, Long accumulator) {
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

class AdCountResult implements WindowFunction<Long, CountByProvince, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<CountByProvince> collector) throws Exception {
        collector.collect(new CountByProvince(String.valueOf(timeWindow.getEnd()), s, iterable.iterator().next()));
    }
}