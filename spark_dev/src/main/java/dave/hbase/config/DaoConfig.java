package dave.hbase.config;

import dave.hbase.dao.ConnectionManager;
import dave.hbase.impl.ConnectionManagerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;


//@Configuration
//@PropertySource("classpath:HBase.properties")
public class DaoConfig {

    @Autowired
    private Environment env;

    @Bean (destroyMethod = "closeAll")
    public ConnectionManager connectionManager(){
        return new ConnectionManagerImpl(
                parseIntOrDefault(env.getProperty("hbase.min.connections"), 1)
                , parseIntOrDefault(env.getProperty("hbase.max.connections"), 5));
    }

    public static int parseIntOrDefault(String str, int defVal){
        try{
            return Integer.parseInt(str);
        }
        catch(NumberFormatException e){
            return defVal;
        }
    }

}