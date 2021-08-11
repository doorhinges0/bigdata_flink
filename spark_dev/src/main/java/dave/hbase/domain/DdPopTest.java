package dave.hbase.domain;

public class DdPopTest {
    private Long population;
    private String state;
    private String city;

    public Long getPopulation() {
        return population;
    }

    public void setPopulation(Long population) {
        this.population = population;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "DdPopTest{" +
                "population=" + population +
                ", state='" + state + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}