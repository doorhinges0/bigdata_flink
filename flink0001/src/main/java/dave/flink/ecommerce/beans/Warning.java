package dave.flink.ecommerce.beans;

public class Warning {

    private long userId;
    private long firstFailTime;
    private long lastFailTime;
    private String warningMsg;

    @Override
    public String toString() {
        return "Warning{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailTime=" + lastFailTime +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }

    public Warning(long userId, long firstFailTime, long lastFailTime, String warningMsg) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailTime;
        this.warningMsg = warningMsg;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getFirstFailTime() {
        return firstFailTime;
    }

    public void setFirstFailTime(long firstFailTime) {
        this.firstFailTime = firstFailTime;
    }

    public long getLastFailTime() {
        return lastFailTime;
    }

    public void setLastFailTime(long lastFailTime) {
        this.lastFailTime = lastFailTime;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }
}
