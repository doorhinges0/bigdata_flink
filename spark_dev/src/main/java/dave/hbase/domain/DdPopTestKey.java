package dave.hbase.domain;

public class DdPopTestKey {
    private String state;

    private String city;

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state == null ? null : state.trim();
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city == null ? null : city.trim();
    }


    public static void main(String[] args) {

    }

}

class Dest {
    private String state;
    private String city;
}

