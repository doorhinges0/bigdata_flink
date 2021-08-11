package dave.spark.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;
import java.util.List;

public class Spark01_CreateRDD_Mem {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("CreateRDD").set("spark.testing.memory", "2000000000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        ArrayList<String> list = new ArrayList();
        list.add("hello");
        list.add("spark");
        list.add("hello1");
        list.add("flink");

        JavaRDD<String> rdd = sc.parallelize(list, 3);
//         makeRDD 没有这个方法
//        JavaRDD<String> rdd2 = sc.(list);



        rdd.saveAsTextFile("E:\\flink_dev\\spark_dev\\output");
//        List<String> res =  rdd.collect();

        System.out.println(rdd.partitions().size());
//        for (String one: res) {
//            System.out.println(one);
//        }

        sc.stop();
    }
}
