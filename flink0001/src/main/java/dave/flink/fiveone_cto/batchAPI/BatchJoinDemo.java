package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class BatchJoinDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> data = new ArrayList<Tuple2<Integer, String>>();
        data.add(Tuple2.of(1, "bj"));
        data.add(Tuple2.of(2, "hz"));
        data.add(Tuple2.of(3, "gz"));
        List<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();
        data2.add(Tuple2.of(1, "dave"));
        data2.add(Tuple2.of(2, "tom"));
        data2.add(Tuple2.of(3, "jack"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        text.join(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                })
                .print();

        text.join(text2).where(0)
                .equalTo(0)
                .map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                        return new Tuple3<>(value.f0.f0, value.f0.f1, value.f1.f1);
                    }
                })
                .print("map=");

//        ds.distinct().print("distinct");
        env.execute();
    }
}
