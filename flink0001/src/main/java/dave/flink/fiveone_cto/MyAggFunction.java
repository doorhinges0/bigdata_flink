package dave.flink.fiveone_cto;

import dave.flink.fiveone_cto.beans.AuditInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MyAggFunction implements WindowFunction<AuditInfo, Tuple4<String, String, String, Long>, Tuple2<String, String>, TimeWindow> {

    @Override
    public void apply(Tuple2<String, String> tuple2, TimeWindow timeWindow, Iterable<AuditInfo> iterable, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {
            String type = tuple2.f0;
            String area = tuple2.f1;
            List<Long> list = new ArrayList<Long>();

            Iterator<AuditInfo> it = iterable.iterator();
            long count = 0L;
            while (it.hasNext()) {
                    AuditInfo auditInfo = it.next();
                    count++;
                    list.add(auditInfo.getTimestamp());
            }
            Collections.sort(list);
            SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
            String timestamp = "";
            try {
                timestamp = sdf.format(list.get(list.size() - 1)) + ",";
                timestamp += sdf.format(list.get(0));
            } catch (Exception e) {
                e.printStackTrace();
            }
            collector.collect(Tuple4.of(timestamp, type, area, count));
    }
}

