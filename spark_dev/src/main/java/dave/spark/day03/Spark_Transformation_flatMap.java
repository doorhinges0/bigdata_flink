package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_flatMap {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<List<Integer>>  list = new ArrayList<List<Integer>>();
        list.add(Arrays.asList(1, 2));
        list.add(Arrays.asList(3, 4, 8));
        list.add(Arrays.asList(5, 6, 9, 99));

        JavaRDD<List<Integer>> rdd = sc.parallelize(list, 2);
        JavaRDD<String> newRDD = rdd.flatMap(new FlatMapFunction<List<Integer>, String>() {
            @Override
            public Iterator<String> call(List<Integer> integers) throws Exception {
                List<String> list = new ArrayList<>();
                list.add("========" + integers.size());
                for (int i = 0; i < integers.size(); i++) {
                    list.add("," + integers.get(i).toString());
                }
                return list.iterator();
            }
        });
        
        newRDD.collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String integer) {
                System.out.println(integer);
            }
        });

    }
}
