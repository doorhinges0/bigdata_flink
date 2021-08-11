package dave.flink.fiveone_cto.custormsource;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<Long> {

    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("numPartitions====" + numPartitions);
        if (key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
