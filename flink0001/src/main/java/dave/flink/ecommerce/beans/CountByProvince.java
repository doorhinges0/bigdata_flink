package dave.flink.ecommerce.beans;

public class CountByProvince {

    public String windowEnd;
    public String province;
    public long count;

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountByProvince{" +
                "windowEnd=" + windowEnd +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }

    public CountByProvince(String windowEnd, String province, long count) {
        this.windowEnd = windowEnd;
        this.province = province;
        this.count = count;
    }
}
