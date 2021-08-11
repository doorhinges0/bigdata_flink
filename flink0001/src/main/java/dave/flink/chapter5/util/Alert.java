package dave.flink.chapter5.util;

public class Alert {

    public String message;
    public long timestamp;

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public Alert() {
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
