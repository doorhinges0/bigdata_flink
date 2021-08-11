package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchDemoHashRange {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer, String>> data = new ArrayList<Tuple2<Integer, String>>();
        data.add(new Tuple2<>(2, "bj"));
        data.add(new Tuple2<>(4, "sh"));
        data.add(new Tuple2<>(3, "sz"));
        data.add(new Tuple2<>(2, "bj1"));
        data.add(new Tuple2<>(4, "sh1"));
        data.add(new Tuple2<>(3, "sz1"));
        data.add(new Tuple2<>(2, "bj2"));
        data.add(new Tuple2<>(4, "sh2"));
        data.add(new Tuple2<>(3, "sz2"));
        data.add(new Tuple2<>(1, "gz"));
        data.add(new Tuple2<>(1, "wh"));
        data.add(new Tuple2<>(1, "dg"));
        data.add(new Tuple2<>(5, "gz1"));
        data.add(new Tuple2<>(5, "wh2"));
        data.add(new Tuple2<>(5, "dg5"));
        data.add(new Tuple2<>(6, "gz1"));
        data.add(new Tuple2<>(6, "wh2"));
        data.add(new Tuple2<>(6, "dg5"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
//        text.first(3).print();
        System.out.println("==========================");
//        text.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
//            @Override
//            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
//                Iterator<Tuple2<Integer, String>> it = values.iterator();
//                while (it.hasNext()) {
//                    Tuple2<Integer, String> one = it.next();
//                    System.out.println("===============" + Thread.currentThread().getId() + "," + one);
//                }
//            }
//        }).print();

        text.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()) {
                    Tuple2<Integer, String> one = it.next();
                    System.out.println("===============" + Thread.currentThread().getId() + "," + one);
                }
            }
        }).print();


        env.execute();
    }
}
