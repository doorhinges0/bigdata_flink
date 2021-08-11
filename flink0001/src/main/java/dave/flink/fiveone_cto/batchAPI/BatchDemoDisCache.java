package dave.flink.fiveone_cto.batchAPI;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class BatchDemoDisCache {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        env.registerCachedFile("E:\\flink_dev\\flink0001\\src\\main\\resources\\AdClickLog.csv", "AdClickLog.csv");

        DataSource<String> data = env.fromElements("bj", "bj8", "bj3", "1231");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            private String content = "";
            @Override
            public void open(Configuration parameters) throws Exception {
                File myFile = getRuntimeContext().getDistributedCache().getFile("AdClickLog.csv");
                List<String> line = FileUtils.readLines(myFile);
                content = line.get(0);
                System.out.println("============line==============" + content + "," + 11/2 );
            }

            @Override
            public String map(String value) throws Exception {
                System.out.println("============map==============" + content);
                return value;
            }
        }).setParallelism(8);

        result.print("=====ccccc");

        System.out.println("==========================");
        JobExecutionResult jobExecutionResult = env.execute();
    }
}
