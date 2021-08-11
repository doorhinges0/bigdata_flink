package dave.spark.day06;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark_Accumulator {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "dave.spark.day05.MyKryoRegistrator");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Accumulator<Double> accumulator = sc.doubleAccumulator(0.0);














    }
}
