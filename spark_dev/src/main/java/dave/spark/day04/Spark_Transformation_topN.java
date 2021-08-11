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
import java.util.*;
import java.util.function.Consumer;

public class Spark_Transformation_topN {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

//        List<SensorReading> list = SensorSourceFixedGenetator.getSRList(4);
//        List<Tuple2<String, Double>> list = SensorSourceFixedGenetator.getSRPair(4);
//        JavaRDD<SensorReading> rdd = sc.parallelize(list, 3);


        JavaRDD<String> rdd0 = sc.textFile("E:\\flink_dev\\spark_dev\\src\\main\\resources\\agent.log");
        JavaRDD<Tuple2<String, Integer>> rdd1 = rdd0.map(new Function<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(String v1) throws Exception {
                String[] str = v1.split(" ");
                return new Tuple2<>((str[1] + "-" + str[4]), 1);
            }
        });
        rdd1.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println("rdd1" + stringIntegerTuple2);
            }
        });
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._1, stringIntegerTuple2._2);
            }
        });
        rdd2.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println("rdd2" + stringIntegerTuple2);
            }
        });

        JavaPairRDD < String, Integer > rdd3 = rdd2.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        rdd3.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
                System.out.println("rdd3" + stringIntegerTuple2);
            }
        });

        JavaRDD<Tuple2<String, String>> rdd4 =  rdd3.map(new Function<Tuple2<String, Integer>, Tuple2<String, String>>() {

            private static final long serialVersionUID = 2297388533716722487L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Integer> v1) throws Exception {
                String[] provinceAds = v1._1.split("-");
                return new Tuple2<>(provinceAds[0], (provinceAds[1] + "," + v1._2));
            }
        });

        rdd4.collect().forEach(new Consumer<Tuple2<String, String>>() {
            @Override
            public void accept(Tuple2<String, String> stringStringTuple2) {
                System.out.println("rdd4:key=" + stringStringTuple2._1 +",value=" + stringStringTuple2._2);
            }
        });

        JavaPairRDD<String, Tuple2<String, String>> rdd5 = rdd4.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] str = stringStringTuple2._2.split(",");
                Tuple2<String, String> value = new Tuple2<>(str[0], str[1]);
                return new Tuple2<>(stringStringTuple2._1, value);
            }
        });
        rdd5.collect().forEach(new Consumer<Tuple2<String, Tuple2<String, String>>>() {
            @Override
            public void accept(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) {
                System.out.println("rdd5:key=" + stringTuple2Tuple2._1 +",value=" + stringTuple2Tuple2._2);
            }
        });
        JavaPairRDD<String, Iterable<Tuple2<String, String>>> rdd6 = rdd5.groupByKey();
        rdd6.collect().forEach(new Consumer<Tuple2<String, Iterable<Tuple2<String, String>>>>() {
            @Override
            public void accept(Tuple2<String, Iterable<Tuple2<String, String>>> stringIterableTuple2) {
                System.out.println("rdd6:key=" + stringIterableTuple2._1 +",value=" + stringIterableTuple2._2);
            }
        });

        JavaPairRDD<String, List<AdTimes>> rdd7 = rdd6.mapValues(new Function<Iterable<Tuple2<String, String>>, List<AdTimes>>() {
            @Override
            public List<AdTimes> call(Iterable<Tuple2<String, String>> v1) throws Exception {
                List<AdTimes> list = new ArrayList<>();
                for (Tuple2<String, String> one: v1) {
                    list.add(new AdTimes(one._1, one._2));
                };
                Collections.sort(list);
                return Arrays.asList(list.get(0), list.get(1), list.get(2));
            }
        });
        rdd7.collect().forEach(new Consumer<Tuple2<String, List<AdTimes>>>() {
            @Override
            public void accept(Tuple2<String, List<AdTimes>> stringListTuple2) {
                System.out.println("rdd6:key=" + stringListTuple2._1 +",value=" + stringListTuple2._2);
            }
        });
//        JavaPairRDD<String, Tuple2<String, String>> rdd7 = rdd6.mapValues(new Function<Iterable<Tuple2<String, String>>, Tuple2<String, String>>() {
//
//            @Override
//            public Tuple2<String, String> call(Iterable<Tuple2<String, String>> v1) throws Exception {
//                List<AdTimes> list = new ArrayList<>();
//                for (Tuple2<String, String> one: v1) {
//                    list.add(new AdTimes(one._1, one._2));
//                };
//                Collections.sort(list);
//            }
//        });

        try {
            Thread.sleep(99999999L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        sc.stop();
    }

    static class AdTimes  implements Serializable, Comparable<AdTimes> {
        @Override
        public String toString() {
            return "AdTimes{" +
                    "ADNo='" + ADNo + '\'' +
                    ", times='" + times + '\'' +
                    '}';
        }

        public AdTimes(String ADNo, String times) {
            this.ADNo = ADNo;
            this.times = times;
        }

        private String ADNo;
        private String times;

        public String getADNo() {
            return ADNo;
        }

        public void setADNo(String ADNo) {
            this.ADNo = ADNo;
        }

        public String getTimes() {
            return times;
        }

        public void setTimes(String times) {
            this.times = times;
        }

        @Override
        public int compareTo(AdTimes o) {
            return o.times.compareTo(this.times);
        }
    }

}
