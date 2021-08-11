package dave.hbase.dao;

public interface ConnectionManager <T>{
    T getConnection();
    void releaseConnection(T connection);
    void closeAll();
}

