package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.ArrayList;
import java.util.List;

public class BatchDemoFirstN {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> data = new ArrayList<Tuple2<Integer, String>>();
        data.add(new Tuple2<>(2, "bj"));
        data.add(new Tuple2<>(4, "sh"));
        data.add(new Tuple2<>(3, "sz"));
        data.add(new Tuple2<>(2, "bj1"));
        data.add(new Tuple2<>(4, "sh1"));
        data.add(new Tuple2<>(3, "sz1"));
        data.add(new Tuple2<>(1, "gz"));
        data.add(new Tuple2<>(1, "wh"));
        data.add(new Tuple2<>(1, "dg"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
//        text.first(3).print();
        System.out.println("==========================");
        text.groupBy(0).first(1).print("group==");
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("==========================");
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(2).print("==");
        System.out.println("==========================");
        env.execute();
    }
}
