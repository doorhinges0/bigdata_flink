package dave.flink.chapter5;

import dave.flink.chapter5.util.Alert;
import dave.flink.chapter5.util.SmokeLevel;
import dave.flink.chapter5.util.SmokeLevelSource;
import dave.flink.util.SensorReading;
import dave.flink.util.SensorSource;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

public class MultiStreamTransformations {

    public static void main(String[] args) throws  Exception {

        /*StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> tempReadings = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());*/

        String hostName = "192.168.74.100";
        int port = 9000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> tempReadings = env.socketTextStream(hostName, port, "\n").map(new MapFunction<String, SensorReading>() {
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

        // 另外一种写法
        tempReadings.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(5*1000)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.timestamp * 1000;
            }
        });

        DataStream<SmokeLevel> smokeLevelDS = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        KeyedStream<SensorReading, String> keyTempReading = tempReadings.keyBy(r -> r.id);

        DataStream<Alert> alertDataStream = keyTempReading
                .connect(smokeLevelDS.broadcast())
                .flatMap(new RaiseAlertFlatMap());

        alertDataStream.print();
        env.execute();


    }

    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

        private SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading value, Collector<Alert> out) throws Exception {

            if (smokeLevel == SmokeLevel.HIGH && value.temperature > 100){
                out.collect(new Alert("risk of fire!" + value, value.timestamp));
            }
        }

        @Override
        public void flatMap2(SmokeLevel value, Collector<Alert> out) throws Exception {
            smokeLevel = value;
        }

    }

}
