package dave.spark.day01;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.api.java.function.Function2;


public class HelloWorld {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wc");
        conf.set("spark.testing.memory", "2000000000");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> text = jsc.textFile("E:\\flink_dev\\spark_dev\\src\\main\\resources\\helloworld.txt", 3);
        System.out.println("Begin to split!");
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                System.out.println("flatMap once, line is " + line );
                String[] wordsArray = line.split(" ");
                List<String> wordsList = Arrays.asList(wordsArray);
                return wordsList.iterator();
            }
        });

//        List<String> strList = words.collect();

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

//        List<Tuple2<String, Integer>> tuple2s = ones.collect();

        System.out.println("Begin to reduce!");
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        counts.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println(stringIntegerTuple2);
            }
        });
        List<Tuple2<String, Integer>> tuple2counts = counts.collect();
        System.out.println("cont");
        for (Tuple2<String, Integer> cont: tuple2counts) {
            System.out.println(cont);
        }

        text.saveAsTextFile("E:\\flink_dev\\spark_dev\\output");

//        Thread.sleep(10);
        jsc.stop();
    }

}
