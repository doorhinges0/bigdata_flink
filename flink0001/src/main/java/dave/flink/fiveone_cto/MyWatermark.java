package dave.flink.fiveone_cto;

import dave.flink.fiveone_cto.beans.AuditInfo;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MyWatermark implements AssignerWithPeriodicWatermarks<AuditInfo> {

    Long currentMaxTimestamp = 0L;
    Long maxOutOfOrderness = 1000L;
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(AuditInfo auditInfo, long prev) {
        long timestamp = auditInfo.getTimestamp()/*0L*/;
        if ( timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        return timestamp;
    }
}
