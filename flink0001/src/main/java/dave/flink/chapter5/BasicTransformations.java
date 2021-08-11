package dave.flink.chapter5;


import dave.flink.util.SensorReading;
import dave.flink.util.SensorSource;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class BasicTransformations {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000L);

        String hostName = "192.168.74.100";
        int port = 9000;

        String s11 = "SensorReading{id='sensor_11', timestamp=1606550539000, temperature=49.15563370353584}";


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
//                return (SensorReading) value;
            }
        })/*;*/
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

//        sourceStream.print();
        DataStream<SensorReading> filterReadings  = sourceStream.filter( r -> r.temperature >= 50);
        filterReadings.print();
        System.out.println("===============================");
        DataStream<String> sensorIds = filterReadings.map(r -> r.id);
        sensorIds.print();
        /*DataStream<SensorReading> readingDataStream = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());*/

//        DataStream<SensorReading> filterReadings = sourceStreamreadingDataStream
//                .filter(r -> r.temperature >= 30);

//        DataStream<String> sensorIds = filterReadings
//                .map(r -> r.id);

        DataStream<String> splitIds = sensorIds.flatMap((FlatMapFunction<String, String>)(id, out) -> { for (String s: id.split("_")){
            out.collect(s);
        } }).returns(Types.STRING());

        splitIds.print();

        env.execute("basic transformations example");

    }

    public static class TemperatureFilter implements FilterFunction<SensorReading> {

        private final double threshold;

        public TemperatureFilter(double threshold) {
            this.threshold = threshold;
        }

        @Override
        public boolean filter(SensorReading sensorReading) throws Exception {
            return sensorReading.temperature >= threshold;
        }
    }

    public static class IdExtractor implements MapFunction<SensorReading, String> {

        @Override
        public String map(SensorReading value) throws Exception {
            return value.id;
        }
    }

    public static class IdSplitter implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {

            String[] splits = value.split("_");

            for (String split : splits) {
                out.collect(split);
            }

        }
    }



}
