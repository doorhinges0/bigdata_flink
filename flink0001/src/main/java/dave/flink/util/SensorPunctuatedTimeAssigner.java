package dave.flink.util;


import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class SensorPunctuatedTimeAssigner implements AssignerWithPunctuatedWatermarks<SensorReading> {


    private long bound = 60*1000L;

    @Override
    public Watermark checkAndGetNextWatermark(SensorReading lastElement, long extractedTimestamp) {

        if (lastElement.id == "sensor_1") {
            return new Watermark(extractedTimestamp - bound);
        } else
            return null;
    }

    @Override
    public long extractTimestamp(SensorReading sensorReading, long l) {
        return sensorReading.timestamp;
    }
}
