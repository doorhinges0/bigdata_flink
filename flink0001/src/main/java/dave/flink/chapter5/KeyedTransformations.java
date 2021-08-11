package dave.flink.chapter5;

import dave.flink.util.SensorReading;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedTransformations {

    public static void main(String[]  args) throws  Exception {

        String hostName = "192.168.74.100";
        int port = 9000;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sourceStream = env.socketTextStream(hostName, port, "\n").map(new MapFunction<String, SensorReading>() {
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

        KeyedStream<SensorReading, String> keyed = sourceStream.keyBy(r -> r.id);

        DataStream<SensorReading> maxTempPerSensor = keyed.reduce((r1, r2) ->{
            if (r1.temperature > r2.temperature){
                return r1;
            } else {
                return  r2;
            }
        });

        maxTempPerSensor.print();
        env.execute("keyed transformations");
    }

}
