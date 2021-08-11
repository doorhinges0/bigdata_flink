package dave.hbase.impl;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dave.hbase.dao.ConnectionManager;

public class ConnectionManagerImpl implements ConnectionManager<Connection>{
    private final Logger LOGGER = LoggerFactory.getLogger(ConnectionManagerImpl.class);

    private final BlockingQueue<Connection> availableConnectionQueue;
    private final ConcurrentHashMap<Connection, Long> connectionMap;
    private final ConcurrentLinkedQueue<Connection> toRemoveQueue;
    private final int minConnections;
    private final int maxConnections;

    public ConnectionManagerImpl(int minConnections, int maxConnections){
        this.minConnections = minConnections;
        this.maxConnections = maxConnections;
        LOGGER.info("The number of connections will be from "+minConnections + " to "+maxConnections);

        availableConnectionQueue = new ArrayBlockingQueue<>(maxConnections);
        toRemoveQueue = new ConcurrentLinkedQueue<>();
        connectionMap = new ConcurrentHashMap<>(maxConnections);
        addNewConnections(minConnections);
        LOGGER.info(minConnections + " connections have been created.");
        new ConnMaintenThread().start();
    }

    @Override
    public Connection getConnection() {
        try{
            Connection conn =  availableConnectionQueue.take(); //blocking if empty
            return conn;
        }catch(InterruptedException e){
            LOGGER.error("Get connection gets interrupted", e);
            throw new RuntimeException(e);
        }
    }

    private Connection createConnection(){
        try {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "moon.rts.com");
            return ConnectionFactory.createConnection(conf);
            //return ConnectionFactory.createConnection();
        } catch (IOException e) {
            LOGGER.error("Connection pool creation failed", e);
            throw new RuntimeException(e);
        }
    }

    private void addNewConnections(int num){
        for(int i=0; i<num; i++){
            Connection conn = createConnection();
            availableConnectionQueue.add(conn);
            connectionMap.put(conn, new Date().getTime());
        }
    }

    private void removeConnections(){
        while(!toRemoveQueue.isEmpty()){
            Connection conn = toRemoveQueue.poll();
            connectionMap.remove(conn);
            try {
                conn.close();
            } catch (IOException e) {
                LOGGER.error("Closing connection failed: ", e);
            }
        }
    }

    @Override
    public void releaseConnection(Connection connection) {
        if(connection == null) return;
        if(connection.isClosed() || availableConnectionQueue.size()>=minConnections){
            toRemoveQueue.offer(connection);
        }
        else{
            if(!availableConnectionQueue.offer(connection)){
                toRemoveQueue.offer(connection);
            }
        }
    }

    @Override
    public void closeAll() {
        for(Connection conn : connectionMap.keySet()){
            try {
                conn.close();
            } catch (IOException e) {
                LOGGER.error("Closing connection failed", e);
            }
        }
    }

    private class ConnMaintenThread extends Thread{

        @Override
        public void run(){
            try{
                while(true){
                    sleep(1000*5);
                    //if available connection < min and total connections < max, add new connections
                    if(availableConnectionQueue.size()<minConnections && connectionMap.size()<maxConnections){
                        addNewConnections(minConnections - availableConnectionQueue.size());
                    }
                    removeConnections();
                }
            }
            catch(InterruptedException e){
                LOGGER.error("onnMainThread got interrupted");
            }
        }
    }

}





