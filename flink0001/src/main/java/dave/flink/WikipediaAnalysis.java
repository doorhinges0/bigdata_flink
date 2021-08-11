package dave.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class WikipediaAnalysis {

    public static void main(String[]  args)  {
        System.out.println("hello scala 4 java");

        try {
            StreamExecutionEnvironment str = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource dss = str.fromElements("xiaomi", "redmi", "oppo", "realme", "vivo", "oppo realme", "realme x7 pro");
            DataStream<Tuple2<String, Integer>> ds = dss.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    for (String word: value.split("\\s")){
                        out.collect(new Tuple2<String, Integer>(word,1));
                    }
                }
            }).setParallelism(2)/*.slotSharingGroup("flatMap_sg")*/.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                    return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                }
            }).setParallelism(3)/*.slotSharingGroup("sum_sg")*//*.print()*//*.sum(1).print()*/;
            ds.print();
//            ds.keyBy(0).sum(1).print();
            str.execute("finished ");
        }catch (Exception exception){
            System.out.println(exception.getMessage());
            System.out.println(exception.getStackTrace());
        }


    }



}
