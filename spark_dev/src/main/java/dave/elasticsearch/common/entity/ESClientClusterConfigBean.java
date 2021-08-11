package dave.elasticsearch.common.entity;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ESClientClusterConfigBean implements ApplicationContextAware {

    private ApplicationContext content;

    public String host;
    public int port;
    public String scheme;
    public String token;
    public String charSet;
    public int connectTimeOut;
    public int socketTimeout;
    public String pass;
    public String user;
    public String certFilePath;


    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.content = applicationContext;
    }
}
