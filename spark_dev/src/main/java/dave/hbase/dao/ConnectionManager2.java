package dave.hbase.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.*;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager2 {
    private Map<String, Connection> connectionMap = new ConcurrentHashMap<>();

    public Connection getConnection(String resourceId, Configuration configuration) {
//        ResourceInfo resourceInfo = ResourceInfoCache.getResourceInfoByCache(resourceId);
//        if (resourceInfo == null) {
//            throw new IllegalArgumentException("error resourceid: " + resourceId);
//        }
//        String key = getClusterKey(resourceInfo);
        String key = null;
        if (connectionMap.containsKey(key)) {
            return connectionMap.get(key);
        }
        synchronized (this) {
            //DCL检查
            if (connectionMap.containsKey(key)) {
                return connectionMap.get(key);
            }
            Connection connection = null;
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                return null;
            }
            connectionMap.put(key, connection);
            return connection;
        }
    }

    @PreDestroy
    public void doDestroy() {
        for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
            Connection connection = entry.getValue();
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    //。。。。
                }
            }
        }
    }
}
