package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_mapPartition {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<Integer>  list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);

        JavaRDD<Integer> rdd = sc.parallelize(list, 2);
        JavaRDD<Integer> newRDD = rdd.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                List<Integer> list = new ArrayList<>();
                while (integerIterator.hasNext()) {
                    Integer one = integerIterator.next() * 4;
                    list.add(one);
                }
                return list.iterator();
            }
        });

        newRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
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

        newRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 5;
            }
        }).collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println("======" + integer);
            }
        });
        newRDD.collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println(integer);
            }
        });

    }
}
