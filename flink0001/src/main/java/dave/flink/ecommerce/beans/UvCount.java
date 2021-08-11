package dave.flink.ecommerce.beans;

public class UvCount {

    private Long windowEnd;
    private Long uvCount;

    public UvCount(Long windowEnd, Long uvCount) {
        this.windowEnd = windowEnd;
        this.uvCount = uvCount;
    }

    @Override
    public String toString() {
        return "UvCount{" +
                "windowEnd=" + windowEnd +
                ", uvCount=" + uvCount +
                '}';
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }
}
