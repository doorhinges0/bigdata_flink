package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.OrderEvent;
import dave.flink.ecommerce.beans.ReceiptEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TxMatchByJoin {

    private static OutputTag<OrderEvent> outputTagUnmatchPay = new OutputTag<OrderEvent>("unmatchpay"){};
    private static OutputTag<ReceiptEvent> outputTagUnmatchRecipt = new OutputTag<ReceiptEvent>("unmatchrecipt"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        KeyedStream<OrderEvent, String> orderds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] line = value.split(",");
                        return new OrderEvent(Long.valueOf(line[0].trim()), line[1].trim(), line[2].trim(), Long.valueOf(line[3].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(OrderEvent loginEvent) {
                        return loginEvent.getEventTime() * 1000L;
                    }
                })
                .filter( data -> (!data.getTxId().equals("")))
                .keyBy(data -> data.getTxId());

        KeyedStream<ReceiptEvent, String> receiptds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\ReceiptLog.csv")
                .map(new MapFunction<String, ReceiptEvent>() {
                    @Override
                    public ReceiptEvent map(String value) throws Exception {
                        String[] line = value.split(",");
                        return new ReceiptEvent(line[0].trim(), line[1].trim(), Long.valueOf(line[2].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ReceiptEvent>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(ReceiptEvent loginEvent) {
                        return loginEvent.getEventTime() * 1000L;
                    }
                })
                .keyBy(data -> data.getTxId());

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> singleOutputStreamOperator = orderds.intervalJoin(receiptds)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new TxPayMatchByJoin());

        singleOutputStreamOperator.print("match=");
        env.execute();
    }

    static class TxPayMatchByJoin extends ProcessJoinFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        @Override
        public void processElement(OrderEvent orderEvent, ReceiptEvent receiptEvent, Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            collector.collect(Tuple2.of(orderEvent, receiptEvent));
        }
    }

    static class TxPayMatch extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        private ValueState<OrderEvent> payEventValueState;
        private ValueState<ReceiptEvent> receiptEventValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay-state", OrderEvent.class));
            receiptEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("recipt-state", ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            ReceiptEvent receiptEvent= receiptEventValueState.value();
            if (receiptEvent != null) {
                out.collect(Tuple2.of(pay, receiptEvent));
                receiptEventValueState.clear();
            } else {
                payEventValueState.update(pay);
                ctx.timerService().registerEventTimeTimer(pay.getEventTime() * 1000L + 5000L);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            OrderEvent pay = payEventValueState.value();
            if (pay != null){
                out.collect(Tuple2.of(pay, receipt));
                payEventValueState.clear();
            } else {
                receiptEventValueState.update(receipt);
                ctx.timerService().registerEventTimeTimer(receipt.getEventTime() * 1000L + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            if (payEventValueState.value() != null) {
                ctx.output(outputTagUnmatchPay, payEventValueState.value());
            }
            if (receiptEventValueState.value() != null) {
                ctx.output(outputTagUnmatchRecipt, receiptEventValueState.value());
            }
            payEventValueState.clear();
            receiptEventValueState.clear();
        }
    }
}
