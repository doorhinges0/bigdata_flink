package dave.flink.ecommerce.beans;

public class OrderResult {
    private Long orderId;
    private String resultMsg;

    public OrderResult(Long orderId, String resultMsg) {
        this.orderId = orderId;
        this.resultMsg = resultMsg;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId='" + orderId + '\'' +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getResultMsg() {
        return resultMsg;
    }

    public void setResultMsg(String resultMsg) {
        this.resultMsg = resultMsg;
    }
}
