package dave.spark.day07;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Spark_sql {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkSession spark = SparkSession.builder().appName("Spark_sql")
                .config(conf).getOrCreate();

//        runBasicDataFrameExample(spark);
//        runDatasetCreationExample(spark);
        runProgrammaticSchemaExample(spark);
        spark.stop();
    }

    private static void runProgrammaticSchemaExample(SparkSession spark) throws AnalysisException {

        JavaRDD<String> peopleRDD = spark.sparkContext().textFile("E:\\flink_dev\\spark_dev\\src\\main\\resources\\people.txt", 2).toJavaRDD();
        String schemaString = "name age";

        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] atts = v1.split(",");
                return RowFactory.create(atts[0], atts[1].trim());
            }
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
        peopleDataFrame.show();
        peopleDataFrame.createOrReplaceTempView("people");
        Dataset<Row> results = spark.sql("select * from people");
        results.show();
       Dataset<String> nameDS =  results.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                return "name:" + value.getString(0);
            }
        }, Encoders.STRING());
       System.out.println("===============nameDS.show()==============");
       nameDS.show();
    }

    private static void runDatasetCreationExample(SparkSession spark) throws  AnalysisException {

        Person person = new Person();
        person.setAge(19);
        person.setName("dave");

        Encoder<Person> personEncoders = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoders);
        javaBeanDS.show();

        Dataset<Person> df = spark.read().json("E:\\flink_dev\\spark_dev\\src\\main\\resources\\people.json").as(personEncoders);
        df.show();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {

        Dataset<Row> df = spark.read().json("E:\\flink_dev\\spark_dev\\src\\main\\resources\\people.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("select * from people");
        System.out.println("select * from people ");
        sqlDF.show();

        df.createGlobalTempView("people");
        System.out.println("select * from global_temp.people");
        spark.newSession().sql("select * from global_temp.people").show();

    }

    public static class Person implements Serializable {

        private static final long serialVersionUID = -6785949597560205071L;

        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
