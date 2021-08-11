package dave.flink.fiveone_cto.batchAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BatchDemoCounter {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("bj", "bj8", "bj3", "1231");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            IntCounter intCounter = new IntCounter();
            int sum = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("num-lines", intCounter);
            }

            @Override
            public String map(String value) throws Exception {
                sum++;
                intCounter.add(1);
                System.out.println("============sum==============" + sum);
                return value;
            }
        }).setParallelism(8);

        result.print("=====ccccc");

        System.out.println("==========================");
        JobExecutionResult jobExecutionResult = env.execute();
        int num = jobExecutionResult.getAccumulatorResult("num-lines");
        System.out.println("==============num============" + num);
    }
}
