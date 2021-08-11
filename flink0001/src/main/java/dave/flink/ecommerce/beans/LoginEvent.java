package dave.flink.ecommerce.beans;

public class LoginEvent {

    private long userId;
    private String ip;
    private String eventType;
    private long eventTime;

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + eventTime +
                '}';
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public LoginEvent(long userId, String ip, String eventType, long timestamp) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.eventTime = timestamp;
    }
}
