package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;

public class Spark_Transformation_groupBy_WordCountEnhance {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Tuple2<String, Integer>  list1 = new Tuple2<String, Integer>("hello world spark scala", 2);
        Tuple2<String, Integer>  list2 = new Tuple2<String, Integer>("hello flink spark", 3);
        Tuple2<String, Integer>  list3 = new Tuple2<String, Integer>("hello dave scala", 3);
        List<Tuple2<String, Integer>>  list = Arrays.asList(list1, list2, list3);
//        Arrays.asList("hello world spark scala", 2);

        JavaRDD<Tuple2<String, Integer>> rdd = sc.parallelize(list, 2);

        JavaRDD<Tuple2<String, Integer>> flapMapRDD = rdd.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                int value = stringIntegerTuple2._2;
                String[] keys = stringIntegerTuple2._1.split(" ");
                for (int i = 0; i < keys.length; i++) {
                    list.add(new Tuple2<>(keys[i], value));
                }
                return list.iterator();
            }
        });

        flapMapRDD.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println(stringIntegerTuple2);
            }
        });

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = flapMapRDD.<String>groupBy(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> v1) throws Exception {
                return v1._1;
            }
        });

        JavaRDD<Tuple2<String, Integer>> resRDD;
        resRDD =
                groupRDD.map(new Function<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> v1) throws Exception {
                        int count = 0;
                        for (Tuple2<String, Integer> one : v1._2) {
                            count += one._2;
                        }
                        return new Tuple2<>(v1._1, count);
                    }
                });

        resRDD.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println("===" + stringIntegerTuple2);
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
