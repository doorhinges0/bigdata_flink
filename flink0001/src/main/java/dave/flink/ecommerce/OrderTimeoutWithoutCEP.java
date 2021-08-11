package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.OrderEvent;
import dave.flink.ecommerce.beans.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class OrderTimeoutWithoutCEP {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        DataStream<OrderResult> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] line = value.split(",");
                        return new OrderEvent(Long.valueOf(line[0].trim()), line[1].trim(), line[2].trim(), Long.valueOf(line[3].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(OrderEvent loginEvent) {
                        return loginEvent.getEventTime() * 1000L;
                    }
                })
                .keyBy(data -> data.getOrderId())
                .process(new KeyedProcessFunction<Long, OrderEvent, OrderResult>() {

                    private ValueState<Boolean> isPayedState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("ispayed-state", Boolean.class));
//                        isPayedState.update(false);
                    }

                    @Override
                    public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
                        Boolean isPayed = isPayedState.value();
                        if (isPayed == null) {
                            isPayedState.update(false);
                            isPayed = false;
                        }
                        if (((!isPayed)) && value.getEventType().trim().equals("create")) {
                            ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L + 15 * 60 * 1000L);
                        } else if (value.getEventType().trim().equals("pay")) {
                            isPayedState.update(true);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
                        Boolean isPayed = isPayedState.value();
                        if (isPayed) {
                            out.collect(new OrderResult(ctx.getCurrentKey(), "order payed successfully"));
                        } else {
                            out.collect(new OrderResult(ctx.getCurrentKey(), "order timeout"));
                        }
                        isPayedState.clear();
                    }

                });

//        DataStream<OrderResult> orderResultDataStream = ds.process(new OrderTimeoutWarning());

        ds.print();
        env.execute();
    }


    static class OrderTimeoutWarning extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        private ValueState<Boolean> isPayedState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("ispayed-state", Boolean.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            Boolean isPayed = isPayedState.value();
            if ((isPayed == null) && value.getEventType().trim().equals("create")) {
                ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L + 15 * 60 * 1000L);
            } else if (value.getEventType().trim().equals("pay")) {
                isPayedState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            Boolean isPayed = isPayedState.value();
            if (isPayed) {
                out.collect(new OrderResult(ctx.getCurrentKey(), "order payed successfully"));
            } else {
                out.collect(new OrderResult(ctx.getCurrentKey(), "order timeout"));
            }
            isPayedState.clear();
        }

    }

}
