package dave.flink.util;

import java.io.Serializable;

public class SensorReading /*implements Serializable*/ {

    private  static final long  serialVersionUID = 122222222222222L;

    public String id;

    public long timestamp;

    public double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
