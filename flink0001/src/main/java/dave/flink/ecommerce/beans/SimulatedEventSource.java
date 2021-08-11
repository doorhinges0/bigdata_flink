package dave.flink.ecommerce.beans;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import javax.naming.ldap.PagedResultsControl;
import java.util.*;

public class SimulatedEventSource extends RichSourceFunction<MarketingUserBehavior> {

    private Boolean running =true;
    private List<String> behaviorTypes = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
    private List<String> channelSets = Arrays.asList("wechat", "weibo", "appstore", "huaweistore");
    private Random rand = new Random(100);

    @Override
    public void run(SourceContext ctx) throws Exception {
        long maxElements = Long.MAX_VALUE;
        long count = 0L;

        while (running && count < maxElements){
            String id = UUID.randomUUID().toString();
            String behavior = behaviorTypes.get(Math.abs(rand.nextInt()) % behaviorTypes.size());
            String channel = channelSets.get(Math.abs(rand.nextInt()) % channelSets.size());
            long ts = System.currentTimeMillis();

            ctx.collect(new MarketingUserBehavior(id, behavior, channel, ts));
            count++;
            Thread.sleep(10L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
