package dave.hbase.demo;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseDemo {
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.74.110:2181,192.168.74.111:2181,192.168.74.112:2181");
    }

    /**
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     *
     */
    public static boolean isExist(String tableName) throws IOException {
        // 老API
        // HBaseAdmin admin = new HBaseAdmin(conf);
        // 新API
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // return admin.tableExists(Bytes.toBytes(tableName));
        return admin.tableExists(TableName.valueOf(tableName));
    }

    // HBase表的创建
    public static void createTable(String tableName, String... columnFamliy) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (isExist(tableName)) {
            System.out.println("表已经存在");
        } else {
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamliy) {
                htd.addFamily(new HColumnDescriptor(cf));
            }

            // 创建表
            admin.createTable(htd);
            System.out.println("创建成功");
        }
    }

    // 删除表
    public static void deleteTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if (isExist(tableName)) {
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("delete sucessfully");
        } else {
            System.out.println("not exist");
        }
    }
    //插入一行数据
    public static void addRow(String tableName,String rowKey,String cf,String column,String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }
    //删除一行数据
    public static void deleteRow(String tableName,String rowKey,String cf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }
    //删除多行数据
    public static void deleMultiRow(String tableName,String... rowKeys) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<>();
        for (String row : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
    }
    //扫描数据
    public static void getAllRows(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("rowKey: "+ Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("cf: "+ Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("cn: "+ Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value: "+ Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("--------------------");
            }
        }
    }
    //得到一个具体数据
    public static void getRow(String tableName,String rowKey) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
//		get.addFamily(Bytes.toBytes("info1"));
        get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("sex"));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowKey: "+ Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("cf: "+ Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("cn: "+ Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("value: "+ Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("--------------------");
        }

    }
    // 在主函数中进行测试
    public static void main(String[] args)  {

        try {
            Configuration conf =  HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.74.110:2181,192.168.74.111:2181,192.168.74.112:2181");

            //ddl的操作对象
            HBaseAdmin admin = new HBaseAdmin(conf);
            TableName[] tableNames = admin.listTableNames();
            //表名
            TableName name = TableName.valueOf("student_test");
            HTableDescriptor desc = new HTableDescriptor(name);

            //列族
            HColumnDescriptor base_info = new HColumnDescriptor("base_info");
            HColumnDescriptor extra_info = new HColumnDescriptor("extra_info");
            //最大保存的历史版本个数
            base_info.setMaxVersions(5);

            desc.addFamily(base_info);
            desc.addFamily(extra_info);
            //创建表
//            admin.createTable(desc);
            admin.close();

    //		System.out.println(isExist("student"));
    //		createTable("staff1", "info1","info2");
    //		deleteTable("student");
//            addRow("staff", "1001", "info1", "sex", "male");
    //		addRow("staff", "1002", "info2", "name", "haha");
    //		addRow("staff", "1004", "info2", "name", "haha");
    //		deleteRow("staff", "1001", null);
    //		deleMultiRow("staff", "1001","1002");
    //		getAllRows("staff");
//            getRow("staff","1001");

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}