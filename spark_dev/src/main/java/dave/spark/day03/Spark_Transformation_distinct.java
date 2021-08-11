package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_distinct {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        List<String> strList = Arrays.asList("hello world spark scala", "2", "dave", "we");
        List<Integer> strList = Arrays.asList(1, 2, 3, 4, 12, 13, 14, 22, 23, 24, 32, 33, 34, 1, 2, 3, 4);


        JavaRDD<Integer> rdd = sc.parallelize(strList, 2);

        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            private static final long serialVersionUID = -1698004048250432093L;

            @Override
            public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
                while (v2.hasNext()) {
                    System.out.println("=11==" + v1 + ", " + v2.next());
                }
                return v2;
            }
        }, true).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("=123==" + integer);
            }
        });
        

        rdd.distinct()/*.collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer s) {
                System.out.println("===" + s);
            }
        })*/.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Integer>>() {
            private static final long serialVersionUID = -1698004048250432093L;

            @Override
            public Iterator<Integer> call(Integer v1, Iterator<Integer> v2) throws Exception {
                while (v2.hasNext()) {
                    System.out.println("=12==" + v1 + ", " + v2.next());
                }
                return v2;
            }
        }, true).collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println("===" + integer);
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
