package dave.flink.fiveone_cto;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import dave.flink.fiveone_cto.beans.AuditInfo;
import dave.flink.fiveone_cto.beans.UsertypeInfo;
import dave.flink.util.JsonSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DataReport {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        OutputTag<AuditInfo> outputTag = new OutputTag<AuditInfo>("late-data") {};

        System.out.println("availableProcessors===========" + Runtime.getRuntime().availableProcessors());

        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> ds = env.addSource(new JsonSource())
                .map(new MapFunction<String, AuditInfo>() {
                    @Override
                    public AuditInfo map(String value) throws Exception {
                        AuditInfo usertypeInfo = JSON.parseObject(value, new TypeReference<AuditInfo>() {});
                        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                        long timestamp = 0;
                        try {
                            timestamp = sdf.parse(usertypeInfo.getDt()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        usertypeInfo.setTimestamp(timestamp);
                        return usertypeInfo;
                    }
                })
                .assignTimestampsAndWatermarks(new MyWatermark())
                .keyBy(new KeySelector<AuditInfo, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(AuditInfo value) throws Exception {
                        return Tuple2.of(value.getType(), value.getArea());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(outputTag)
                .apply(new MyAggFunction());

        DataStream<AuditInfo> latenessDS= ds.getSideOutput(outputTag);
        latenessDS.print("lateness data==");
        ds.print("ds==");
        env.execute("json test");
    }
}
