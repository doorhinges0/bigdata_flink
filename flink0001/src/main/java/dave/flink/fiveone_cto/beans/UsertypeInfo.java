package dave.flink.fiveone_cto.beans;

import java.io.Serializable;

public class UsertypeInfo implements Serializable {

    private static final long serialVersionUID = -7398122211657560825L;
    private String dt;
    private String type;
    private String username;
    private String area;

    @Override
    public String toString() {
        return "UsertypeInfo{" +
                "dt='" + dt + '\'' +
                ", type='" + type + '\'' +
                ", username='" + username + '\'' +
                ", area='" + area + '\'' +
                '}';
    }

    public UsertypeInfo(String dt, String type, String username, String area) {
        this.dt = dt;
        this.type = type;
        this.username = username;
        this.area = area;
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
