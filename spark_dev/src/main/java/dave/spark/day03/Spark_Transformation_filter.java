package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_filter {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> strList = Arrays.asList("hello world spark scala", "2", "dave", "we");

        JavaRDD<String> rdd = sc.parallelize(strList, 2);

        rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.contains("a");
            }
        }).collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println("===" + s);
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
