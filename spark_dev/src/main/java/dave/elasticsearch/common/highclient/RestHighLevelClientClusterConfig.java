package dave.elasticsearch.common.highclient;

import com.alibaba.fastjson.JSONObject;
import dave.elasticsearch.common.entity.ESClientClusterConfigBean;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.springframework.core.env.Environment;


/**
 * @author
 */
@Configuration
@PropertySource("classpath:es-config.properties")
public class RestHighLevelClientClusterConfig {

    @Bean(name="esClientClusterConfigBean")
    public ESClientClusterConfigBean getRedisConfigBean(Environment env) throws Exception {

//        Config config = ConfigService.getAppConfig();
//        String value = config.getProperty(REDIS_KEY, null);
//        Map<String, String> mapNew = (Map) JSONObject.parseObject(value, Map.class);
//
        ESClientClusterConfigBean configBean = new ESClientClusterConfigBean();
        configBean.host = "192.168.74.111;192.168.74.110;192.168.74.112";
        configBean.user = "";
        configBean.pass = "";
        configBean.port = 9200;
        configBean.certFilePath = "";
        return configBean;
    }


    private static SSLContext createSslContext(String certFilePath)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException, UnrecoverableKeyException {

        final File certFile = new File(certFilePath);
        final FileInputStream certStream = new FileInputStream(certFile);

        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        X509Certificate cert = (X509Certificate) certificateFactory.generateCertificate(certStream);
        String alias = cert.getSubjectX500Principal().getName();

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null);
        trustStore.setCertificateEntry(alias, cert);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(trustStore, null);
        KeyManager[] keyManagers = kmf.getKeyManagers();

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
        tmf.init(trustStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, null);

        return sslContext;
    }

    @Bean(name="RestHighLevelClientCluser")
    public RestHighLevelClient restHighLevelClient(@Qualifier(value="esClientClusterConfigBean") ESClientClusterConfigBean  configBean) {

        boolean sslEnabled = false;
        String certFilePath = configBean.certFilePath;
        String user = configBean.user;
        String pass = configBean.pass;
        int elasticPort = configBean.port;
        String[] strHosts = configBean.host.split(";");
        List<String> elasticHosts = Arrays.asList(strHosts)/*new ArrayList<String>()*/;

        String scheme = sslEnabled ? "https" : "http";

        HttpHost[] hosts = elasticHosts
                .stream()
                .map(host -> new HttpHost(host, elasticPort, scheme))
                .toArray(HttpHost[]::new);

        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pass));

        RestClientBuilder restClientBuilder = RestClient.builder(hosts)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider);
                    if(sslEnabled) {
                        SSLContext sslContext = null;
                        try {
                            sslContext = createSslContext(certFilePath);
                        } catch (KeyStoreException | IOException | NoSuchAlgorithmException | CertificateException | KeyManagementException | UnrecoverableKeyException e) {
                            e.printStackTrace();
                        }
                        httpClientBuilder
                                .setSSLContext(sslContext);
                    }
                    return httpClientBuilder;
                });

        return new RestHighLevelClient(restClientBuilder);
    }
}
