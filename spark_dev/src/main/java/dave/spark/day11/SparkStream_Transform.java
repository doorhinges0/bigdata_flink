package dave.spark.day11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkStream_Transform {

    public static void main(String[] args) throws Exception {

        String hostName = "192.168.74.100";
        int port = 9000;

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostName, port);
//        JavaDStream<Tuple2<String, Integer>>  dStream = lines.transform(new Function<JavaRDD<String>, JavaRDD<Tuple2<String, Integer>>>() {
//            @Override
//            public JavaRDD<Tuple2<String, Integer>> call(JavaRDD<String> v1) throws Exception {
//                return null;
//            }
//        });

        // 返回的是String, 可以自行spilt 转化为需要的结构
        JavaDStream<String> javaDStream = lines.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> javaRDD) throws Exception {
                JavaRDD<String>  javaRDD1 = javaRDD.flatMap(new FlatMapFunction<String, String>() {
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
                JavaPairRDD<String, Integer> javaPairRDD =  javaRDD1.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
                JavaRDD<String> javaPairRDD1 = javaPairRDD.sortByKey().map(new Function<Tuple2<String, Integer>, String>() {
                    @Override
                    public String call(Tuple2<String, Integer> v1) throws Exception {
                        return v1._1 + "," + v1._2;
                    }
                });
                return  javaPairRDD1;
            }
        });
        javaDStream.print();
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
