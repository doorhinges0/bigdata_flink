package dave.flink.training;

import dave.flink.util.SensorReading;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowTest {

    public static void main(String[] args)  throws  Exception {

        String hostName = /*"localhost"*/"192.168.74.91";
        int port = 8765;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(100L);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig();
//        env.getCheckpointConfig(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(50), org.apache.flink.api.common.time.Time.seconds(20)));
        env.getCheckpointConfig().setCheckpointTimeout(30000);
//        env.setStateBackend();


        DataStream<SensorReading> sensorReadingDataStream = env.socketTextStream(hostName, port, "\n").map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {

                String[] ss = value.split("\\'");
                String[] equals = value.split("\\=");
                String s_id = ss[1];
                String s_timestamp  = equals[2].split("\\,")[0].substring(0,equals[2].split("\\,")[0].length());
                String[] ss2= equals[3].split("\\,");
                String s_temperature = ss2[0].substring(0,ss2[0].length()-1);
                return new SensorReading(s_id, Long.valueOf(s_timestamp), Double.valueOf(s_temperature));
            }
        });
              /*  .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {

            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                Long res = sensorReading.timestamp;
                return res;
            }
        });*/

        /*.assignTimestampsAndWatermarks(new MyAssigner());*/
        /*.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {

            @Override
            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                return element.timestamp;
            }
        }));*/
        /*.assignTimestampsAndWatermarks(new WatermarkStrategy<SensorReading>() {
            @Override
            public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<SensorReading>() {
                    @Override
                    public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {

                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {

                    }
                };
            }
        });*/

//                .assignTimestampsAndWatermarks(new MyAssigner());

        DataStream<SensorReading>  minDS = sensorReadingDataStream.keyBy(r -> r.id)
                .timeWindow(Time.seconds(10))
                .reduce((r1, r2) -> {
                    if (r1.temperature > r2.temperature){
                        return new SensorReading("min_"+r2.id, r2.timestamp, r2.temperature);
                    } else {
                        return new SensorReading("min_"+r1.id, r1.timestamp, r1.temperature);
                    }
                });

//        System.out.println("===========2442");
//        sensorReadingDataStream.print();
        minDS.print();
        env.execute();

        sensorReadingDataStream.keyBy(r -> r.id).process(new MyKeybyFunc());
    }


    static class MyKeybyFunc extends KeyedProcessFunction<String, SensorReading, String> {


        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            ctx.timerService().currentProcessingTime();
        }

    }

    static class MyAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {

        private long bound = 6000L;
        private long maxTs = Long.MIN_VALUE;

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTs - bound);
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            if (maxTs < sensorReading.timestamp)
                maxTs = sensorReading.timestamp;
            return sensorReading.timestamp ;
        }
    }

    static class MyAssigner2 implements AssignerWithPunctuatedWatermarks<SensorReading> {

        private long bound = 60*1000L;

        // 没有水位线就是不用等，到达窗口结束时间直接触发
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {
            //
            if (lastElement.id == "sensor_1") {
                return new Watermark(extractedTimestamp - bound);
            } else
                return null;
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            return sensorReading.timestamp * 1000;
        }

    }

}
