package dave.flink.ecommerce.beans;

public class BlackListWarning {

    private long userId;
    private long adId;
    private String msg;

    @Override
    public String toString() {
        return "BlackListWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", msg='" + msg + '\'' +
                '}';
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getAdId() {
        return adId;
    }

    public void setAdId(long adId) {
        this.adId = adId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public BlackListWarning(long userId, long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }
}
