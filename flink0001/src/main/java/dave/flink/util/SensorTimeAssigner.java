package dave.flink.util;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    //  maxOutOfOrderness time  is  5 seconds
    public SensorTimeAssigner() {
        super(Time.seconds(5));
    }

    // extract timestamp from  stream for calculate
    @Override
    public long extractTimestamp(SensorReading sensorReading) {
        return /*Long.MIN_VALUE*//*Long.MAX_VALUE*/sensorReading.timestamp * 1000;
    }
}
