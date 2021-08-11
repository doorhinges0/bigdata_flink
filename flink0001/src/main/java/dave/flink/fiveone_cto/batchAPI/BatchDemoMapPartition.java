package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchDemoMapPartition {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> data = new ArrayList<String>();
        data.add("hello world");
        data.add("hello flink");
        DataSource<String> text = env.fromCollection(data);
        FlatMapOperator<String, String> ds = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split("\\W+");
                for (String one : split) {
                    System.out.println(one);
                    out.collect(one);
                }
            }
        });

//        DataSet<String> ds = text.mapPartition(new MapPartitionFunction<String, String>() {
//            @Override
//            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
//                Iterator<String> it = values.iterator();
//                while (it.hasNext()) {
//                    String text = it.next();
//                    String[] split = text.split("\\W+");
//                    for (String word : split) {
//                        out.collect(word);
//                    }
//                }
//            }
//        });

        ds.distinct().print("distinct");
//        text.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                return null;
//            }
//        })
        env.execute();
    }
}
