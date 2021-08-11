package dave.hbase;

import dave.hbase.config.DaoConfig;
import dave.hbase.dao.DdPopTestMapper;
import dave.hbase.domain.DdPopTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author
 */
//@SpringBootApplication(scanBasePackages = "dave.hbase")
public class DemoHbaseApplication {

    @Autowired
    DdPopTest ddPopTest;



    public static void main(String[] args) {
//        SpringApplication.run(DemoHbaseApplication.class, args);

        try {
//            new AnnotationConfigApplicationContext();
//            ApplicationContext factory = new AnnotationConfigApplicationContext(DaoConfig.class);
            ApplicationContext factory=new ClassPathXmlApplicationContext("classpath:applicationContext.xml");
            String[] str = factory.getBeanDefinitionNames();


            DdPopTestMapper dao = factory.getBean(DdPopTestMapper.class);
            Map<String, Object> columnMap = new HashMap<>();
            columnMap.put("city", "sz");
            List<DdPopTest> list = dao.selectByMap(columnMap);
            for (int i = 0; i < list.size(); i++) {
                System.out.println(list.get(i).toString());
            }
            System.out.println("==============");
        } catch (Exception e) {
            System.out.println(e);
        }

    }

}
