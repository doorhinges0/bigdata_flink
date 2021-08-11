package dave.spark.day05;

import dave.spark.util.SensorSourceFixedGenetator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class Spark_action {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "dave.spark.day05.MyKryoRegistrator");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 6, 3, 4), 8);
        Integer res = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(res);

        rdd.collect().forEach(new Consumer<Integer>() {
            @Override
            public void accept(Integer integer) {
                System.out.println(integer);
            }
        });

        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("foreach=" + integer);
            }
        });

        System.out.println("count=" + rdd.count());
        System.out.println("first=" + rdd.first());
        System.out.println("first=" + rdd.take(3));
        System.out.println("takeOrdered=" + rdd.takeOrdered(3));

        int res2 = rdd.aggregate(10,
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                });
        System.out.println("aggregate=" + res2);

        int res3 = rdd.fold(10, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println("fold=" + res3);

        List<Tuple2<String, Double>> list = SensorSourceFixedGenetator.getSRPair(4);
        JavaRDD<Tuple2<String, Double>> rdd2 = sc.parallelize(list);
        System.out.println(rdd2.countByValue());
        rdd2.foreach(new VoidFunction<Tuple2<String, Double>>() {
            @Override
            public void call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

            }
        });

    }
}