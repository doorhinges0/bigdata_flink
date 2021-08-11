package dave.flume.interceptor;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MySink extends AbstractSink implements Configurable {

    private String suffix;
    Logger logger = LoggerFactory.getLogger(MySink.class);

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        try {
            Event event = null;
            transaction.begin();
            while (true) {
                event = channel.take();
                if (event != null)
                    break;
                else
                    Thread.sleep(1000L);
            }

            logger.info(new String(event.getBody()) + suffix);
            transaction.commit();
        } catch (Exception e) {
            status = Status.BACKOFF;
            transaction.rollback();
        } finally {
            transaction.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        suffix = context.getString("suffix", "test");
    }

}
