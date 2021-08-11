package dave.spark.util;


import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

public class SensorSourceFixedGenetator {

//    private static int num;
//
//    public SensorSourceFixedGenetator(int num) {
//        this.num = num;
//    }

    public static List<SensorReading> getSRList(int num) {

        List<SensorReading> list = new ArrayList<>();
        Random rand = new Random();
        String[]  sensorIds = new String[10];
        double[]  curFTemp = new double[10];
        for (int i = 0; i < num; i++) {
            sensorIds[i] = "sensor_" + (i);
            curFTemp[i] = 65 + (rand.nextGaussian()*20);
            long curTime = Calendar.getInstance().getTimeInMillis();
            SensorReading sr = new SensorReading(sensorIds[i], curTime, curFTemp[i]);
            list.add(sr);
        }

        for (int i = 0; i < list.size(); i++) {
            System.out.println("======sourcedata====="+ list.get(i));
        }
        return list;
    }

    public static List<SensorReading> getSRListWithSamekey(int num, int groupkey) {

        List<SensorReading> list = new ArrayList<>();
        Random rand = new Random();
        String[]  sensorIds = new String[num];
        double[]  curFTemp = new double[num];
        for (int i = 0; i < num; i++) {
            sensorIds[i] = "sensor_" + (i % groupkey);
            curFTemp[i] = 65 + (rand.nextGaussian()*20);
            long curTime = Calendar.getInstance().getTimeInMillis();
            SensorReading sr = new SensorReading(sensorIds[i], curTime, curFTemp[i]);
            list.add(sr);
        }

        for (int i = 0; i < list.size(); i++) {
            System.out.println("======sourcedata====="+ list.get(i));
        }
        return list;
    }
    public static List<Tuple2<String, Double>>  getSRPair(int num) {

        List<Tuple2<String, Double>> res = new ArrayList<>();
        List<SensorReading> list = getSRList(num);
        for (int i = 0; i < list.size(); i++) {
            SensorReading sensorReading = list.get(i);
            Tuple2 tuple2 = new Tuple2<>(sensorReading.id, sensorReading.temperature);
            System.out.println("======sourcedata Tuple2====="+ list.get(i));
            res.add(tuple2);
        }
        return res;
    }
}
