package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchDemoBroadcast {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> dataBroad = new ArrayList<Tuple2<String, Integer>>();
        dataBroad.add(new Tuple2<>("bj", 1));
        dataBroad.add(new Tuple2<>("bj2", 12));
        dataBroad.add(new Tuple2<>("bj3", 14));
        dataBroad.add(new Tuple2<>("bj5", 15));
        dataBroad.add(new Tuple2<>("bj6", 16));
        dataBroad.add(new Tuple2<>("bj7", 17));
        dataBroad.add(new Tuple2<>("bj8", 18));
        DataSource<Tuple2<String, Integer>> broadDS = env.fromCollection(dataBroad);

        DataSet<HashMap<String, Integer>> tobroadDS = broadDS.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> data = new HashMap<String, Integer>();
                data.put(value.f0, value.f1);
                return data;
            }
        });

        DataSource<String> data = env.fromElements("bj", "bj8", "bj3");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                broadCastMap = getRuntimeContext().getBroadcastVariable("broadcastMap");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + ", age=" + age;
            }
        }).withBroadcastSet(tobroadDS, "broadcastMap");

        result.print("=====ccccc");

        System.out.println("==========================");
        env.execute();
    }
}
