package dave.hbase.dao;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;



public abstract class CommonDaoImpl<V> implements CommonDao<String, V> {
    @Autowired
    private ConnectionManager<Connection> connectionManager;

    protected abstract V parseResult(Result result);
    protected abstract String getTableName();
    protected abstract Put buildRow(String key, V t);


    @Override
    public Map<String, V> findAll() {
        Connection connection = connectionManager.getConnection();
        try(Table table = connection.getTable(TableName.valueOf(getTableName()));
            ResultScanner scanner = table.getScanner(new Scan());) {
            Map<String, V> map = new LinkedHashMap<>();
            for(Result result : scanner){
                map.put(Bytes.toString(result.getRow()), parseResult(result));
            }
            return map;
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        finally{
            connectionManager.releaseConnection(connection);
        }
    }

    @Override
    public V findById(String key) {
        Connection connection = connectionManager.getConnection();
        try(Table table = connection.getTable(TableName.valueOf(getTableName()));) {
            Get get = new Get(Bytes.toBytes(key.toString()));
            Result result = table.get(get);
            if(result.isEmpty()) return null;
            return parseResult(result);

        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        finally{
            connectionManager.releaseConnection(connection);
        }
    }


    @Override
    public void put(String key, V t) {
        Connection connection = connectionManager.getConnection();
        try(Table table = connection.getTable(TableName.valueOf(getTableName()));) {
            Put put = buildRow(key, t);
            table.put(put);
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        finally{
            connectionManager.releaseConnection(connection);
        }

    }

    @Override
    public void delete(String key){
        Connection connection = connectionManager.getConnection();
        try(Table table = connection.getTable(TableName.valueOf(getTableName()));) {
            Delete delete = new Delete(Bytes.toBytes(key));
            table.delete(delete);
        }
        catch(IOException e){
            throw new RuntimeException(e);
        }
        finally{
            connectionManager.releaseConnection(connection);
        }
    }


}