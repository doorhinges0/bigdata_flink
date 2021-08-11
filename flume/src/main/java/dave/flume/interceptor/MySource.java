package dave.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.ArrayList;
import java.util.List;

// 使用flume接收数据，并给每条数据添加前缀，输出到控制台。前缀可从flume配置文件中配置。
public class MySource extends AbstractSource implements Configurable, PollableSource {
    private String prefix;
    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        List<Event> list = new ArrayList<Event>();
        for (int i = 0; i < 5; i++) {
            SimpleEvent simpleEvent = new SimpleEvent();
            simpleEvent.setBody((prefix + "hello" + i).getBytes());
        }
        try {
            ChannelProcessor channelProcessor = getChannelProcessor();
            channelProcessor.processEventBatch(list);
        } catch (Exception e) {
            status = Status.BACKOFF;
        }
        return status;
    }

    //没有数据休息时长
    @Override
    public long getBackOffSleepIncrement() {
        return 2000L;
    }
    //没有数据最大休息时长
    @Override
    public long getMaxBackOffSleepInterval() {
        return 5000L;
    }

    //读取配置获取上下文环境
    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix", "test=");
    }
}
