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

public class Spark_Transformation_groupBy {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer>  list = Arrays.asList(1,2,3,4,5,6,7,8,9);

        JavaRDD<Integer> rdd = sc.parallelize(list, 3);

        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            private static final long serialVersionUID = -8025983527100901675L;
            List<String> list = new ArrayList<>();
            @Override
            public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    if (index % 2 == 0)
                        list.add("partition" + index + ":" + iterator.next()*10);
                    else
                        list.add("partition" + index + ":" + iterator.next());
                }
                return list.iterator();
            }
        }, true).collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String integer) {
                System.out.println("==11====" + integer);
            }
        });


        /*JavaPairRDD<Integer, Iterable<Integer>> newRDD0 = */rdd.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 % 2;
            }
        }).collect().forEach(new Consumer<Tuple2<Integer, Iterable<Integer>>>() {
            @Override
            public void accept(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) {
                System.out.println("==11====" + integerIterableTuple2._1 + integerIterableTuple2._2);
            }
        });


        try {
            Thread.sleep(999999999L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }
}
