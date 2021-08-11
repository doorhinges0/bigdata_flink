package dave.spark.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;


public class Spark_Partition {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wc");
        conf.set("spark.testing.memory", "2000000000");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> text = jsc.textFile("E:\\flink_dev\\spark_dev\\input\\1.txt", 3);
        System.out.println("Begin to split!");

        text.saveAsTextFile("E:\\flink_dev\\spark_dev\\output");

//        Thread.sleep(10);
        jsc.stop();
    }

}
