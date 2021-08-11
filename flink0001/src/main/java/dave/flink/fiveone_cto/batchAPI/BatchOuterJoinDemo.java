package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class BatchOuterJoinDemo {

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

        text.leftOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (second == null) {
                            return new Tuple3<>(first.f0, first.f1, null);
                        } else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                })
                .print("====");



//        ds.distinct().print("distinct");
        env.execute();
    }
}
