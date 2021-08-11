package dave.spark.util;

import java.io.Serializable;

public class SensorReading implements Serializable, Comparable<SensorReading> {

    private static final long serialVersionUID = -5003203128351877146L;

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


    @Override
    public int compareTo(SensorReading o) {
        int res = this.id.compareTo(o.id);
        if (res == 0)
            return (int)(o.temperature - this.temperature);
        else
            return res;
    }
}
