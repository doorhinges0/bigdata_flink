package dave.flink.training;

import dave.flink.util.SensorReading;
import dave.flink.util.SensorSource;
import dave.flink.util.SensorTimeAssigner;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFuncTest {

    public static void main(String[]  args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

//        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());
        DataStream<SensorReading> sensorData2 = env
                .addSource(new SensorSource()/*, TypeInformation.of(SensorReading.class)*/)/*;*/
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        /*sensorData2.keyBy(r -> r.id)
                .process(new TempIncAlert()).print();*/

        SingleOutputStreamOperator<SensorReading> proDS = sensorData2.keyBy(r -> r.id).flatMap(new TempChangeAlert(3.0));
//                .process(new TempChangeAlert(1.8));
//        sensorData2.print("original");
        proDS.print("diff 10.2");
//        sensorData2.print();
        env.execute();

        sensorData2.keyBy(r -> r.id);
    }

    static class TempChangeAlert extends RichFlatMapFunction<SensorReading, SensorReading> {

        private Double threshood;

        public TempChangeAlert(Double threshold) {
            this.threshood = threshold;
        }
        ValueState<Double>  lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>(("lastTemp"), Types.DOUBLE));
        }

        @Override
        public void flatMap(SensorReading value, Collector<SensorReading> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp == null)
                lastTemp = 0.0;
            Double diff = Math.abs(value.temperature - lastTemp);
//            System.out.println("============" + diff);
            if (diff > threshood) {
                System.out.println("============" + value.temperature + "," + lastTemp);
                out.collect(value);
            }
            lastTempState.update(value.temperature);
        }
    }

    public static class TempChangeAlert2 extends KeyedProcessFunction<String, SensorReading, SensorReading> {

        private Double threshood;

        public TempChangeAlert2(Double threshold) {
            this.threshood = threshold;
        }

        ValueState<Double>  lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>(("lastTemp"), Types.DOUBLE));
        }


        @Override
        public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if (lastTemp == null)
                lastTemp = 0.0;
            Double diff = Math.abs(value.temperature - lastTemp);
//            System.out.println("============" + diff);
            if (diff > threshood) {
                System.out.println("============" + value.temperature + "," + lastTemp);
                out.collect(value);
            }
            lastTempState.update(value.temperature);
        }
    }


    public static class TempIncAlert extends KeyedProcessFunction<String, SensorReading, String> {

        ValueState<Double> lastTemp /*= getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE))*/;

        ValueState<Long> currentTimer /*= getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG))*/;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Types.DOUBLE));

            currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {

            Double lastValue = lastTemp.value();
            if (lastValue == null)
                lastValue = 0.0;
            lastTemp.update(value.temperature);
            Long curTimerTs = currentTimer.value();
            if (curTimerTs == null)
                curTimerTs = 0L;

            if (value.temperature > 0 && value.temperature > lastValue && curTimerTs == 0) {
                long timeTs = ctx.timerService().currentProcessingTime();
                currentTimer.update(timeTs);
                ctx.timerService().registerEventTimeTimer( timeTs + 1*1000);
            } else if ( lastValue > value.temperature || lastValue == 0.0){
                ctx.timerService().deleteEventTimeTimer(curTimerTs);
                currentTimer.clear();
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            System.out.println("111111111"+ ctx.getCurrentKey());
            out.collect(ctx.getCurrentKey() + " temperature continue up");
        }

    }


}
