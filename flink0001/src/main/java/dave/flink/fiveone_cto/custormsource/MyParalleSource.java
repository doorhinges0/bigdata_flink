package dave.flink.fiveone_cto.custormsource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MyParalleSource implements ParallelSourceFunction<Long> {

    private boolean isRunning = true;
    private long count = 0L;
    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect((count++));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
