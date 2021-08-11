package dave.spark.day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkStream_Window {

    public static void main(String[] args) throws Exception {

        String hostName = "192.168.74.100";
        int port = 9000;

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));


        ssc.sparkContext().intAccumulator(0);
        ssc.sparkContext().parallelize(Arrays.asList("1", "2")).persist(StorageLevel.MEMORY_AND_DISK_2());
        ssc.checkpoint("./");


        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);
//        JavaDStream<Tuple2<String, Integer>>  dStream = lines.transform(new Function<JavaRDD<String>, JavaRDD<Tuple2<String, Integer>>>() {
//            @Override
//            public JavaRDD<Tuple2<String, Integer>> call(JavaRDD<String> v1) throws Exception {
//                return null;
//            }
//        });

        JavaDStream<String> windowDS = lines.window(Durations.seconds(6), Durations.seconds(3));

        JavaDStream<String> words = windowDS.flatMap(new FlatMapFunction<String, String>() {
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

        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
//                    stringIntegerJavaPairRDD.keys().iterator()
                System.out.println("=========" + stringIntegerJavaPairRDD);
            }
        });

        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }

    public static class Person implements Serializable {

        private static final long serialVersionUID = -6785949597560205071L;

        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

}
