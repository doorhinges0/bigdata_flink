package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.ApacheLogEvent;
import dave.flink.ecommerce.beans.UrlViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Collections;


public class NetworkFlow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\apache.log")
                .map(new MapFunction<String, ApacheLogEvent>() {
                    @Override
                    public ApacheLogEvent map(String value) throws Exception {
                        String[] line = value.split(" ");
                        SimpleDateFormat format2=new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                        Long  timeStamp = format2.parse(line[3]).getTime();
                        return new ApacheLogEvent(line[0].trim(), line[1].trim(), timeStamp, line[5].trim(), line[6].trim());
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getEventTime();
                    }
                })
                .keyBy(data -> data.getUrl())
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(data -> data.getWindowEnd())
                .process(new TopNHotUrls(5));
        
        ds.print();
        env.execute("networkflow");
    }

    static class TopNHotUrls extends KeyedProcessFunction<Long, UrlViewCount, String> {

        private int topSize;
        private ListState<UrlViewCount> urlState;

        public TopNHotUrls(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            urlState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url-state", UrlViewCount.class));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {

            urlState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            List<UrlViewCount> list = new ArrayList<UrlViewCount>();
            for (UrlViewCount rlc: urlState.get()) {
                list.add(rlc);
            }
            Collections.sort(list, new UVCComparator());
            urlState.clear();
            String line = new String();
            line += "time:" + new Timestamp(timestamp - 1) + "\n";
//            for (int i = 0; i < list.size(); i++) {
//                if (list.get(i).getUrl().equals("/favicon.ico"))
//                    System.out.println("================size=" + i + ",time:" + new Timestamp(timestamp -1).toString() + list.get(i).toString());
//            }
            for (int i = 0; i < topSize; i++) {
                UrlViewCount one = list.get(i);
                line += "no=" + (i + 1) ;
                line += ": URL=" + one.getUrl();
                line += " count=" + one.getCount() + "\n";
            }
            line += "======================";
            Thread.sleep(1000);
            out.collect(line);
        }
    }

    static class CountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
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

    public static class UVCComparator implements Comparator<UrlViewCount> {

        @Override
        public int compare(UrlViewCount o1, UrlViewCount o2) {
            return o1.getCount() > o2.getCount() ? -1 :(o1.getCount() < o2.getCount() ? 1 : 0);
        }
    }

    static class WindowResult implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            collector.collect(new UrlViewCount(s, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }
}
