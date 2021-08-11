package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_doubleValue {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        List<String> strList = Arrays.asList("hello world spark scala", "2", "dave", "we");
        List<Integer> strList = Arrays.asList(1, 2, 3, 4);
        List<Integer> strList2 = Arrays.asList(4, 13, 14, 22);


        JavaRDD<Integer> rdd = sc.parallelize(strList, 3);
        JavaRDD<Integer> rdd2 = sc.parallelize(strList2, 3);

        rdd.union(rdd2).collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println(integer);
            }
        });

        rdd.intersection(rdd2).collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println(" intersection " + integer);
            }
        });

        rdd.subtract(rdd2).collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println(" subtract " + integer);
            }
        });

        rdd.zip(rdd2).collect().forEach(new Consumer<Tuple2<Integer, Integer>>() {
            @Override
            public void accept(Tuple2<Integer, Integer> integerIntegerTuple2) {
                System.out.println(" zip " + integerIntegerTuple2);
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
