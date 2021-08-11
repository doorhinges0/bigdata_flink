package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_groupBy_WordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String>  list = Arrays.asList("hello scala", "hello spark spark", "hello world spark scala");

        JavaRDD<String> rdd = sc.parallelize(list, 3);

        JavaRDD<String> flapMapRDD = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<>();
                String[] one = s.split(" ");
                for (int i = 0; i < one.length; i++) {
                    list.add(one[i]);
                }
                return list.iterator();
            }
        });

        JavaRDD<Tuple2<String, Integer>> mapRDD = flapMapRDD.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String v1) throws Exception {
                return new Tuple2<>(v1, 1);
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = mapRDD.<String>groupBy(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1;
            }
        });

        groupRDD.collect().forEach(new Consumer<Tuple2<String, Iterable<Tuple2<String, Integer>>>>() {
            @Override
            public void accept(Tuple2<String, Iterable<Tuple2<String, Integer>>> stringIterableTuple2) {
                System.out.println(stringIterableTuple2);
            }
        });

        JavaRDD<Tuple2<String, Integer>> resRDD;
        resRDD =
        groupRDD.map(new Function<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> v1) throws Exception {
                int count = 0;
                for (Tuple2<String, Integer> one : v1._2) {
                    count++;
                }
                return new Tuple2<>(v1._1, count);
            }
        });

        resRDD.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println(stringIntegerTuple2);
            }
        });


        try {
            Thread.sleep(99999999L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }
}
