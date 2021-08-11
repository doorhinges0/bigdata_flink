package dave.flink.ecommerce;

import dave.flink.ecommerce.beans.LoginEvent;
import dave.flink.ecommerce.beans.Warning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

public class LoginFail {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

        DataStream<LoginEvent> ds = env.readTextFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] line = value.split(",");
                        return new LoginEvent(Long.valueOf(line[0].trim()), line[1].trim(), line[2].trim(), Long.valueOf(line[3].trim()));
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent) {
                        return loginEvent.getEventTime() * 1000L;
                    }
                });

        DataStream<Warning> warningDataStream = ds.keyBy(data -> data.getUserId())
                .process(new LoginWarning(2));

//        ds.print();
        warningDataStream.print();
        env.execute();
    }
}

class LoginWarning extends KeyedProcessFunction<Long, LoginEvent, Warning> {

    private int maxFailTimes;
    private ListState<LoginEvent> loginFailState;

    public LoginWarning(int maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail", LoginEvent.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<Warning> out) throws Exception {
//        if (value.getEventTime() == 1558430842 || (value.getEventTime() == 1558430843) || (value.getEventTime() == 1558430844)){
//            System.out.println(value.toString());
//        }
        if (value.getEventType().trim().equals("fail")) {
            Boolean find = false;
            if (loginFailState.get().iterator().hasNext()) {
                LoginEvent le = loginFailState.get().iterator().next();
                if (value.getEventTime() < (le.getEventTime() + 2)) {
                    out.collect(new Warning(le.getUserId(), le.getEventTime(), value.getEventTime(), "login fails in 2 seconds for 2 times!"));
                }
                loginFailState.clear();
                loginFailState.add(value);
            } else {
                loginFailState.add(value);
            }
        } else {
            loginFailState.clear();
        }
    }

    public void processElement2(LoginEvent value, Context ctx, Collector<Warning> out) throws Exception {
        if (value.getEventType().trim().equals("fail")) {
            Boolean find = false;
            for (LoginEvent le: loginFailState.get()) {
                find = true;
            }
            if (find) {
                ctx.timerService().registerEventTimeTimer(value.getEventTime() * 1000L + 2000L);
            }
            loginFailState.add(value);
        } else {
            loginFailState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Warning> out) throws Exception {
        List<LoginEvent> allLoginFails = new ArrayList<LoginEvent>();
        for (LoginEvent le: loginFailState.get()) {
            allLoginFails.add(le);
        }
        if (allLoginFails.size() >= maxFailTimes) {
            out.collect(new Warning(allLoginFails.get(0).getUserId(), allLoginFails.get(0).getEventTime(), allLoginFails.get(allLoginFails.size() - 1).getEventTime(), "login fails in 2 seconds for " + allLoginFails.size() + " times!"));
        }
        loginFailState.clear();
    }
}