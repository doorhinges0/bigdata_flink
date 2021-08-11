package dave.spark.day05;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_TestLineage {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrator", "dave.spark.day05.MyKryoRegistrator");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello world", "6", "3", "4"), 8);

        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                System.out.println("flatMap once, line is " + line );
                String[] wordsArray = line.split(" ");
                List<String> wordsList = Arrays.asList(wordsArray);
                return wordsList.iterator();
            }
        });
        rdd2.collect();
        String rdd3 = rdd.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD javaPairRDD = rdd2.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s, s);
            }
        });
        JavaPairRDD< String, String > rdd4 = javaPairRDD.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("=============partitioner================" + rdd4.partitioner());;
        rdd4.collect().forEach(new Consumer<Tuple2<String, String>>() {
            @Override
            public void accept(Tuple2<String, String> stringStringTuple2) {
                System.out.println(stringStringTuple2);
            }
        });

//        rdd3.collect();
        System.out.println("rdd" + rdd.toDebugString());
        System.out.println("=============================");
        System.out.println("rdd2" + rdd2.toDebugString());
        System.out.println("=============================");
//        System.out.println("rdd3" + rdd3);

        Thread.sleep(1000* 6000);
    }
}
