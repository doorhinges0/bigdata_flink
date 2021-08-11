package dave.flink;


import dave.flink.util.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketStreamClientTest {

    public static void main(String[]  args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostName = "localhost";
        int port = 8521;

        DataStream<String> sourceStream = env.socketTextStream(hostName, port, "\n");

        /*DataStream<SensorReading> windowCount = sourceStream.flatMap(new FlatMapFunction<String, SensorReading>() {
            int cishu = 0;
            public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                System.out.println("---------------------------------------out2 处理时间" + "当前读入次数第" + cishu + "次");
                String[] splits = value.split("\\s");
                cishu++;
                //long handleTime=System.currentTimeMillis();
                //System.out.println("---------------------------------------out2 处理时间" + "当前读入次数第" + cishu + "次");
                //int i =0;
                for (String w:splits) {

                    System.out.println(w + "开始处理数据时间" + System.currentTimeMillis());
//                    out.collect(new WordWithCount(w,1));
                }
                //cishu++;
            }
        });

        windowCount.print();*/

        sourceStream.print();
        env.execute("");

    }





}
