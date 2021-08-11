package dave.hbase.dao;


import java.util.Map;

public interface CommonDao<K, V> {
    Map<K, V> findAll();
    V findById(K key);
    void put(K key, V t);
    void delete(K key);
}
