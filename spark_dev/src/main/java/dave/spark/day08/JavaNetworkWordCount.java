package dave.spark.day08;

import javafx.util.Duration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JavaNetworkWordCount {

    public static void main(String[] args) throws Exception {

        String hostName = "192.168.74.100";
        int port = 9000;

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                String[] strs = s.split(" ");
                for (int i = 0; i < strs.length; i++) {
                    list.add(strs[i]);
                }
                return list.iterator();
            }
        });

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
