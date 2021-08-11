package dave.flink.fiveone_cto;


import dave.flink.fiveone_cto.beans.UsertypeInfo;
import dave.flink.util.JsonSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.alibaba.fastjson.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;


public class DataClean {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UsertypeInfo> ds = env.addSource(new JsonSource())
                .map(new MapFunction<String, UsertypeInfo>() {
                    @Override
                    public UsertypeInfo map(String value) throws Exception {
                        UsertypeInfo usertypeInfo = JSON.parseObject(value, new TypeReference<UsertypeInfo>() {});
                        return usertypeInfo;
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UsertypeInfo>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(UsertypeInfo s) {
                        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                        long timestamp = 0;
                        try {
                            timestamp = sdf.parse(s.getDt()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return timestamp * 1000L;
                    }
                });

        ds.print("ds==");

        env.execute("json test");
    }
}
