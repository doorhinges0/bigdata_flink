package dave.spark.day06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.util.parsing.json.JSON;

import java.util.function.Consumer;

public class Spark_ReadJson {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "dave.spark.day05.MyKryoRegistrator");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("E:\\flink_dev\\spark_dev\\src\\main\\resources\\test.json");

//        rdd.map(JSON.parseFull());

        rdd.collect().forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println("=====" + JSON.parseFull(s));
                System.out.println(s);
            }
        });


    }
}
