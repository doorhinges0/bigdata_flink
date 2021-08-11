package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.LoginEvent;
import dave.flink.ecommerce.beans.OrderEvent;
import dave.flink.ecommerce.beans.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderTimeout {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        DataStream<OrderEvent> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\OrderLog.csv")
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
                .keyBy(data -> data.getOrderId());

        Pattern<OrderEvent, ?> pattern = Pattern.<OrderEvent>begin("begin").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return orderEvent.getEventType().trim().equals("create");
            }
        })
                .followedBy("follow").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent) throws Exception {
                return orderEvent.getEventType().trim().equals("pay");
            }
        })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> patternStream = CEP.pattern(ds, pattern);
        OutputTag<OrderResult> outputTag = new OutputTag<OrderResult>("orderTimeout"){};

        SingleOutputStreamOperator<OrderResult> resultDataStream= patternStream.select(outputTag,
                new OrderTimeoutSelect(),
                new OrderPaySelect());

        resultDataStream.print();
        resultDataStream.getSideOutput(outputTag).print();
        env.execute();
    }
}

class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {

    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
        long timeoutOrderId = map.get("begin").iterator().next().getOrderId();
        return new OrderResult(timeoutOrderId, "timeout");
    }
}

class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {

    @Override
    public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
        long payedOrderId = map.get("follow").iterator().next().getOrderId();
        return new OrderResult(payedOrderId, " payed successfully");
    }
}