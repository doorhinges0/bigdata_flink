package dave.flume.interceptor;


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.*;
import java.util.List;

/*** 根据body中的内容在headers中添加指定的key-value
 * 如果内容为字母你们就添加 state=letter
 * 如果内容为数字你们就添加 state=number
 */
public class MyInterceptor implements Interceptor {
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if ((body[0] >= 'A' && body[0] >= 'Z') || (body[0] >= 'a' && body[0] >= 'z')) {
            event.getHeaders().put("type", "letter");
        } else  {
            event.getHeaders().put("type", "number");
        }
        return event;
    }
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event: list) {
            intercept(event);
        }
        return list;
    }
    // 会被interceptor实例，参照 TimestampInterceptor 写
    // 注意 1：静态内部类 2：权限要是public
    public static class Builder implements org.apache.flume.interceptor.Interceptor.Builder {
        private boolean preserveExisting = false;
        private String header = "timestamp";
        public Interceptor build() {
            return new MyInterceptor();
        }
        public void configure(Context context) {
        }
    }


    @Override
    public void close() {

    }

    @Override
    public void initialize() {

    }

}
