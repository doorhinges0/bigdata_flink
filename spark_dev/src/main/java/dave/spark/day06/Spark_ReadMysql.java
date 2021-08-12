package dave.spark.day06;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.util.parsing.json.JSON;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

public class Spark_ReadMysql {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("mappartition");
        conf.set("spark.testing.memory", "2000000000");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "dave.spark.day05.MyKryoRegistrator");

        Map<String, String> map = new HashMap<>();
        map.put("driver", "com.mysql.jdbc.Driver");
        map.put("url", "jdbc:mysql://:/");
        map.put("user", "");
        map.put("password", "");
        map.put("dbtable", "user_behavior");
        map.put("useSSL", "false"); //useSSL=true

        JavaSparkContext sc = new JavaSparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);
//        Dataset<Row> df = sqlContext.read().options(map).format("jdbc").load();
//        df.show();

        JavaRDD<String> rdd = JdbcRDD.create(
                sc,
                new JdbcRDD.ConnectionFactory() {
                    @Override
                    public Connection getConnection() throws Exception {
                        return DriverManager.getConnection("jdbc:mysql://47.107.112.168:3306/qywx", "root", "AcadsoC!@#$%^236");
                    }
                },
                "select * from user_behavior where id >= ? and id < ?",
                1,
                Long.MAX_VALUE,
                1,
                new Function<ResultSet, String>() {
                    @Override
                    public String call(ResultSet v1) throws Exception {
                        return v1.getString(2);
                    }
                }
        );

        System.out.println(rdd.collect());

//        Dataset<Row> upDS = new Dataset<Row>();
// spark sql update data
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("jdbc").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaRDD<String> source = sparkSession.read().textFile("E:\\flink_dev\\spark_dev\\src\\main\\resources\\id_test").javaRDD();
        JavaRDD<Row> rowRDD = source.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] parts = v1.split(",");
                String sid = parts[0] + Math.random();
                String sname = parts[1];
                return RowFactory.create(sid, sname);
            }
        });

        ArrayList<StructField> fields = new ArrayList<>();
        StructField field = null;
        field = DataTypes.createStructField("ids", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("values1", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
        df.show();

        //准备配置文件
        final Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "AcadsoC!@#$%^236");
        props.setProperty("useSSL", "false");
        String url = "jdbc:mysql://47.107.112.168:3306/qywx";
        String targetTable = "id_test";

        df.write().mode(SaveMode.Append).jdbc(url, targetTable, props);


// jdbc 格式
//        source.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                String[] parts = s.split(",");
//                String sid = parts[0]  + Math.random();;
//                String sname = parts[1];
//
//                Connection conn = DriverManager.getConnection("jdbc:mysql://47.107.112.168:3306/qywx", "root", "AcadsoC!@#$%^236");
//                String sql = " insert into id_test(ids, values1) values (?,?) ";
//                PreparedStatement psm = conn.prepareStatement(sql);
//                psm.setString(1, sid);
//                psm.setString(2, sname);
//                psm.executeUpdate();
//                psm.close();
//                conn.close();
//            }
//        });



        source.foreachPartition(new VoidFunction<Iterator<String>>() {

            @Override
            public void call(Iterator<String> stringIterator) throws Exception {

                if(stringIterator!=null) {
                    List<String> myList = Lists.newArrayList(stringIterator);

                    Connection con = DriverManager.getConnection("jdbc:mysql://47.107.112.168:3306/qywx", "root", "AcadsoC!@#$%^236");
                    String sql = " insert into id_test(ids, values1) values (?,?) ";
                    boolean success = false;
                    PreparedStatement ps = null;
                    con.setAutoCommit(false);
                    try {
                        ps = con.prepareStatement(sql);

                        int numRcds = myList.size();
                        for (int i = 0; i < numRcds; i++) {

                            String[] parts = myList.get(i).split(",");
                            String sid = parts[0]  + "batch_"+ Math.random();;
                            String sname = parts[1];

                            ps.setString(1, sid);
                            ps.setString(2, sname);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        con.commit();
                        ps.close();
                        con.close();
                    } catch (Exception e) {
                        success = false;
                        System.out.println("Exception inserting  failed:" +  e);
                    } finally {
                        if(ps != null) {try {ps.close();} catch(Exception e){}}
                    }


                }
                System.out.println("===");
            }

        });



        sparkSession.stop();

    }



    /**
     * Convert the list of messages to a byte array to send over as a BLOB to the VTI
     *
     * @param obj
     * @return
     * @throws IOException
     */
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = null;
        ObjectOutputStream o = null;
        byte[] bytes = null;
        try {
            b = new ByteArrayOutputStream();
            o = new ObjectOutputStream(b);
            o.writeObject(obj);
            bytes = b.toByteArray();
        } catch (Exception e) {
            System.out.println("Exception serializing the list of messages." +  e);
        } finally {
            if(o != null) try { o.close(); } catch (Exception e){};
            if(b != null) try { b.close(); } catch (Exception e){};
        }
        return bytes;

    }

}
