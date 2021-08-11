package dave.flink.fiveone_cto.custormsource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class MyRichParalleSource extends RichParallelSourceFunction<Long> {

    private static final long serialVersionUID = 1527676193286460490L;
    private boolean isRunning = true;
    private long count = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("=====================open");
    }

    @Override
    public void close() throws Exception {
        System.out.println("=====================close");
    }

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
