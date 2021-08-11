package dave.flink;

import java.io.*;
import java.net.*;
import java.util.Calendar;
import java.util.Random;
//import java.util.Random;
/**
 * 100ms once
 */
public class SocketServerTest {
    private static final int PORT = 9000;

    public static void test() {
        ServerSocket server = null;
        Socket socket = null;
        DataOutputStream out = null;

        try {
            server = new ServerSocket(PORT);
            socket = server.accept();
            out = new DataOutputStream(socket.getOutputStream());
            int time = 0;
            int num = 0;
            while (true) {
                Thread.sleep(100);
                time = time + 100;
                String MYstr = getRandomStr2();
                System.out.println(MYstr);
                out.writeUTF(MYstr);
//                out.writeBytes(MYstr);
                num = num + 4;
                System.out.println("我发" + MYstr +"序号是" + num + "时间是" + System.currentTimeMillis());
                //System.out.println(System.currentTimeMillis());
                out.flush();
                if (time >= 60000) {
                    System.out.println("我沉睡了" + System.currentTimeMillis());
                    Thread.sleep(12000000);
                    time = 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 生产ItemName随机函数
     * @return
     */
    private static String getRandomStr2() {
        String str = "";

        int q = (int) (Math.random() * 30);
        int x = (int) (Math.random() * 200);
        int y = (int) (Math.random() * 300);
        int z = (int) (Math.random() * 10);

        str = q + " " + x + " " + y + " " + z;
        //System.out.println(str);
        return str;
    }

    private static String getRandomStr() {
        String str = "";

        long curTime = Calendar.getInstance().getTimeInMillis()/1000;

        Random rand = new Random();
        str = "sensor_"+(int) (Math.random()) + "," + (65 + (rand.nextGaussian()*20)) + "," + curTime;
//                System.out.println("======taskIdx====="+ taskIdx);

/*        String[]  sensorIds = new String[10];
        double[]  curFTemp = new double[10];
//        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + ( i);
            curFTemp[i] = 65 + (rand.nextGaussian()*20);
//        }*/

//        System.out.println(str);
        return str;
    }


    public static void main(String[] args) {
        test();
    }
}