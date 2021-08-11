package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_sample {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> strList = Arrays.asList(1, 2, 3, 4, 12, 13, 14, 22, 23, 24, 32, 33, 34, 42, 43, 44, 52, 53, 54, 62, 63, 64);

        JavaRDD<Integer> rdd = sc.parallelize(strList, 2);

       rdd.collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer s) {
                System.out.println("===" + s);
            }
        });

        rdd.sample(true, 1).collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer s) {
                System.out.println("=222==" + s);
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
