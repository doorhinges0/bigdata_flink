package dave.flink.chapter1;

import dave.flink.util.SensorReading;
import dave.flink.util.SensorSource;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class AverageSensorReadings {

    public static  void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

//        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());
        DataStream<SensorReading> sensorData2 = env
                .addSource(new SensorSource()/*, TypeInformation.of(SensorReading.class)*/)/*;*/
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SensorReading>  avgTemp = sensorData2
                .map( r -> new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
//                .map( new MyMapper(), TypeInformation.of(SensorReading.class))
                .keyBy(r->r.id)
                .timeWindow(Time.seconds(5))
                .apply(new TemperatureAverager());

//        sensorData2.print();

        // print result stream to standard out
        avgTemp.print();

        // execute application
        env.execute("Compute average sensor temperature");


    }

     public   static class MyMapper implements MapFunction<SensorReading, SensorReading>{

        @Override
        public SensorReading map(SensorReading r) throws Exception {
            return new SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0));
        }
    }

    public static class TemperatureAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

        /**
         * apply() is invoked once for each window.
         *
         * @param sensorId the key (sensorId) of the window
         * @param window meta data for the window
         * @param input an iterable over the collected sensor readings that were assigned to the window
         * @param out a collector to emit results from the function
         */
        @Override
        public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) {

            // compute the average temperature
            int cnt = 0;
            double sum = 0.0;
            for (SensorReading r : input) {
                cnt++;
                sum += r.temperature;
            }
            double avgTemp = sum / cnt;

            // emit a SensorReading with the average temperature
            out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
        }
    }


}
