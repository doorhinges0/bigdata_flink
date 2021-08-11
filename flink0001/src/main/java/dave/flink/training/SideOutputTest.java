package dave.flink.training;

import dave.flink.util.SensorReading;
import dave.flink.util.SensorSource;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputTest {

    public static void main(String[]  args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

//        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());
        DataStream<SensorReading> sensorData2 = env
                .addSource(new SensorSource()/*, TypeInformation.of(SensorReading.class)*/)/*;*/
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        /*KeyedStream keyedStream =*/
        SingleOutputStreamOperator<SensorReading> ds = sensorData2.process(new FreezingAlert());
        ds.print("normal data==");
        OutputTag<String>  alertOutput = new OutputTag<String>("freezing alert"){};
        ds.getSideOutput(alertOutput).print("alert==");
//        sensorData2.print();
        env.execute();

    }


    static class FreezingAlert extends ProcessFunction<SensorReading, SensorReading> {

        OutputTag<String>  alertOutput = new OutputTag<String>("freezing alert"){};

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {

            if (value.temperature < 32.0) {
                ctx.output(alertOutput, "freezing output" + value.temperature);
            } else {
                out.collect(value);
            }

        }
    }
}
