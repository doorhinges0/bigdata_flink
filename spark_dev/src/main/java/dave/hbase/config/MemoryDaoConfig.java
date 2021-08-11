//package dave.hbase.config;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;
//
//import com.realtimestudio.transport.dao.baisha.CarDao;
//import com.realtimestudio.transport.dao.baisha.DriverDao;
//import com.realtimestudio.transport.dao.baisha.OrganizationDao;
//import com.realtimestudio.transport.dao.baisha.RouteDao;
//import com.realtimestudio.transport.dao.memory.impl.CarDaoImpl;
//import com.realtimestudio.transport.dao.memory.impl.DriverDaoImpl;
//import com.realtimestudio.transport.dao.memory.impl.OrganizationDaoImpl;
//import com.realtimestudio.transport.dao.memory.impl.RouteDaoImpl;
//
//@Configuration
//public class MemoryDaoConfig {
//    @Bean
//    public OrganizationDao organizationDao(){
//        return new OrganizationDaoImpl("../scripts/phoenix/organizations.csv");
//    }
//
//    @Bean
//    @Autowired
//    public DriverDao driverDao(OrganizationDao dao){
//        return new DriverDaoImpl("../scripts/phoenix/drivers.csv", dao);
//    }
//
//    @Bean
//    @Autowired
//    public CarDao carDao(OrganizationDao organDao, DriverDao driverDao){
//        return new CarDaoImpl("../scripts/phoenix/cars.csv", organDao, driverDao);
//    }
//
//    @Bean
//    @Autowired
//    public RouteDao routeDao(DriverDao driverDao, CarDao carDao){
//        return new RouteDaoImpl("../scripts/phoenix/routes.csv", driverDao, carDao);
//    }
//
//}