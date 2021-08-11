package dave.spark.day04;

import dave.spark.util.SensorReading;
import dave.spark.util.SensorSourceFixedGenetator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Spark_Transformation_sortByKey {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("aggregateByKey");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<SensorReading> list = SensorSourceFixedGenetator.getSRListWithSamekey(14, 4);
//        List<Tuple2<String, Double>> list = SensorSourceFixedGenetator.getSRPair(4);
        JavaRDD<SensorReading> rdd = sc.parallelize(list, 6);


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

        javaPairRDD.sortByKey(new MyComparator(), true).collect().forEach(new Consumer<Tuple2<String, Double>>() {
            @Override
            public void accept(Tuple2<String, Double> stringDoubleTuple2) {
                System.out.println("sortByKey =" + stringDoubleTuple2);
            }
        });

        JavaPairRDD<SensorReading, Double> javaPairRDD2 = rdd.mapToPair(new PairFunction<SensorReading, SensorReading, Double>() {
            @Override
            public Tuple2<SensorReading, Double> call(SensorReading sensorReading) throws Exception {
                return new Tuple2<SensorReading, Double>(sensorReading, sensorReading.temperature);
            }
        });

        javaPairRDD2.sortByKey().collect().forEach(new Consumer<Tuple2<SensorReading, Double>>() {
            @Override
            public void accept(Tuple2<SensorReading, Double> sensorReadingDoubleTuple2) {
                System.out.println("sortByKey2 =" + sensorReadingDoubleTuple2);
            }
        });

        JavaPairRDD<String, SensorReading> javaPairRDD3 = rdd.mapToPair(new PairFunction<SensorReading, String, SensorReading>() {
            @Override
            public Tuple2<String, SensorReading> call(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.id, sensorReading);
            }
        });

        javaPairRDD3.sortByKey().collect().forEach(new Consumer<Tuple2<String, SensorReading>>() {
            @Override
            public void accept(Tuple2<String, SensorReading> stringSensorReadingTuple2) {
                System.out.println("sortByKey3 =" + stringSensorReadingTuple2);
            }
        });

        javaPairRDD.combineByKey(
                new Function<Double, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> call(Double v1) throws Exception {
                        return new Tuple2<>(v1, 1);
                    }
                },
                new Function2<Tuple2<Double, Integer>, Double, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Double v2) throws Exception {
                        return new Tuple2<>(v1._1 + v2, v1._2 + 1);
                    }
                },
                new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> call(Tuple2<Double, Integer> v1, Tuple2<Double, Integer> v2) throws Exception {
                        return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2);
                    }
                }).collect().forEach(new Consumer<Tuple2<String, Tuple2<Double, Integer>>>() {
            @Override
            public void accept(Tuple2<String, Tuple2<Double, Integer>> stringTuple2Tuple2) {
                System.out.println("combineByKey =" + stringTuple2Tuple2);
            }
        });



        try {
            Thread.sleep(99999999L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }


    static class MyComparator implements Comparator<String>, Serializable {

        private static final long serialVersionUID = -2710382647542955331L;

        @Override
        public int compare(String o1, String o2) {
            return o2.compareTo(o1);
        }
    }
}
