package dave.spark.day04;

import dave.spark.util.SensorReading;
import dave.spark.util.SensorSourceFixedGenetator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_reduceByKey {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<SensorReading> list = SensorSourceFixedGenetator.getSRList(4);
//        List<Tuple2<String, Double>> list = SensorSourceFixedGenetator.getSRPair(4);
        JavaRDD<SensorReading> rdd = sc.parallelize(list, 3);

        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<SensorReading>, Iterator<SensorReading>>() {
                                       @Override
                                       public Iterator<SensorReading> call(Integer index, Iterator<SensorReading> iterator) throws Exception {
                                           List<SensorReading> list = new ArrayList<>();
                                           while (iterator.hasNext()) {
                                               SensorReading sr = iterator.next();
                                               System.out.println("partition" + index + ":" + sr);
                                               list.add(sr);
                                           }
                                           return list.iterator();
                                       }
                                   }, true).collect();


        JavaPairRDD<String, Double> javaPairRDD = rdd.mapToPair(new PairFunction<SensorReading, String, Double>() {
            @Override
            public Tuple2<String, Double> call(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.id, sensorReading.temperature);
            }
        });

        javaPairRDD.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        }).collect().forEach(new Consumer<Tuple2<String, Double>>() {
            @Override
            public void accept(Tuple2<String, Double> stringDoubleTuple2) {
                System.out.println(stringDoubleTuple2);
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
