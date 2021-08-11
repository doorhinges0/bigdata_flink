package dave.flink.fiveone_cto.beans;

import java.io.Serializable;

public class AuditInfo implements Serializable {


    private static final long serialVersionUID = -2604769891522366470L;
    private String dt;
    private String type;
    private String username;
    private String area;
    private long timestamp;

    @Override
    public String toString() {
        return "AuditInfo{" +
                "dt='" + dt + '\'' +
                ", type='" + type + '\'' +
                ", username='" + username + '\'' +
                ", area='" + area + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    public AuditInfo(String dt, String type, String username, String area, long timestamp) {
        this.dt = dt;
        this.type = type;
        this.username = username;
        this.area = area;
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }
}
