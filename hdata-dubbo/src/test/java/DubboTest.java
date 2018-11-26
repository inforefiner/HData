import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.merce.woven.data.rpc.DataService;

import java.util.Properties;

public class DubboTest {

    public static void main(String[] args) {


        ApplicationConfig application = new ApplicationConfig();
        application.setName("hdata-dubbo-data-writer");
        RegistryConfig registry = new RegistryConfig();
        registry.setProtocol("zookeeper");
        registry.setClient("curatorx");

        registry.setAddress("192.168.1.229:2181");
        registry.setUsername("admin");
        registry.setPassword("123456");

        ReferenceConfig<DataService> reference = new ReferenceConfig<DataService>();
        reference.setApplication(application);
        reference.setRegistry(registry); // 多个注册中心可以用setRegistries()
        reference.setInterface(DataService.class);
        reference.setTimeout(60 * 1000);

        try {
            DataService dataService = reference.get();
            for (int i = 0; i < 10; i++) {
                dataService.prepare("default", "task #" + i, new Properties());
            }

        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("can't connect registry rpc-data-service", e);
        }
    }
}
