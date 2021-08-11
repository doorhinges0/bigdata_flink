package dave.spark.day03;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_glom {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<List<Integer>>  list = new ArrayList<List<Integer>>();
        list.add(Arrays.asList(1, 2));
        list.add(Arrays.asList(3, 4, 8));
        list.add(Arrays.asList(5, 6, 9, 99));

        List<Integer>  singleList = new ArrayList<Integer>();
        List<Integer>  singleList21 =  Arrays.asList(1,2,3,4,5,6,7);

        JavaRDD<Integer> rdd = sc.parallelize(singleList21, 2);

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


        JavaRDD<List<Integer>> rddNew = rdd.glom();
        rddNew.mapPartitionsWithIndex(new Function2<Integer, Iterator<List<Integer>>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<List<Integer>> iterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    if (index % 2 == 0)
                        list.add("partition" + index + ":" + iterator.next()+10);
                    else
                        list.add("partition" + index + ":" + iterator.next());
                }
                return list.iterator();
            }
        }, true).collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String integer) {
                System.out.println("==222====" + integer);
            }
        });
    }
}
