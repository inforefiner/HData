package com.inforefiner.hdata;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class TestClient {


    private static Logger logger = LoggerFactory.getLogger(TestClient.class);

    private Configuration conf;

    protected final static String APPMASTER_JAR = "/Users/joey/HData/hdata-cloud/target/hdata-cloud-0.2.8.jar";

    @Before
    public void setUp() {
        conf = new Configuration();
        String hadoop_conf_dir = System.getenv("HADOOP_CONF_DIR");
        if (StringUtils.isNotBlank(hadoop_conf_dir)) {
            logger.info("HADOOP_CONF_DIR using {}", hadoop_conf_dir);
            conf.addResource(new Path(hadoop_conf_dir, "core-site.xml"));
            conf.addResource(new Path(hadoop_conf_dir, "hdfs-site.xml"));
            conf.addResource(new Path(hadoop_conf_dir, "yarn-site.xml"));
        } else {
            logger.info("HADOOP_HOME not config, using DEFAULT");
        }
    }

    @Test
    public void test2() {
//        String path = "hdfs://tmp/hdata/conf/hdata.cfg";
//        int idx = path.indexOf("conf");
//        System.out.println(path.substring(idx));
//        System.out.println(System.getenv("foo"));
//        System.out.println(System.getProperty("foo"));
        System.out.println(ApplicationMaster.class.getClassLoader().getResource("."));
    }

    @Test
    public void test1() {
        String[] args = {
                "--jar",
                APPMASTER_JAR,
                "--num_containers",
                "1",
                "--shell_command",
//                "ls -lR | nc localhost 8765",
                "./bin/hdata --reader mongodb -Raddress%3D192.168.1.188 -Rusername%3D -Rpassword%3D -Rdatabase%3Directoryperdb -Rcollection%3Dboard_rank_map -Rcolumns%3Dboard_id%2Crank%2Cupdated_at%2Ccreated_at -Rparallelism%3D4 --writer console -Wdebug%3Dtrue",
                "--master_memory",
                "256",
                "--master_vcores",
                "1",
                "--container_memory",
                "512",
                "--container_vcores",
                "1"
        };
        try {
            final SubmitClient client = new SubmitClient(conf);
            boolean initSuccess = client.init(args);
            logger.info("initSuccess : " + initSuccess);
            boolean ret = client.run();
            logger.info("Client run completed. Result=" + ret);
//            final AtomicBoolean result = new AtomicBoolean(false);
//            Thread t = new Thread() {
//                public void run() {
//                    try {
//                        result.set(client.run());
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//            };
//            t.start();
//            YarnClient yarnClient = YarnClient.createYarnClient();
//            yarnClient.init(conf);
//            yarnClient.start();
//            t.join();
//            String hostName = NetUtils.getHostname();
//            boolean verified = false;
//            String errorMessage = "";
//            while (!verified) {
//                List<ApplicationReport> apps = yarnClient.getApplications();
//                if (apps.size() == 0) {
//                    Thread.sleep(10);
//                    continue;
//                }
//                ApplicationReport appReport = apps.get(0);
//                if (appReport.getHost().equals("N/A")) {
//                    Thread.sleep(10);
//                    continue;
//                }
//                errorMessage =
//                        "Expected host name to start with '" + hostName + "', was '"
//                                + appReport.getHost() + "'. Expected rpc port to be '-1', was '"
//                                + appReport.getRpcPort() + "'.";
//                if (checkHostname(appReport.getHost()) && appReport.getRpcPort() == -1) {
//                    verified = true;
//                }
//                if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED) {
//                    break;
//                }
//                logger.info(errorMessage);
//                t.join();
//                logger.info("Client run completed. Result=" + result);
//
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private boolean checkHostname(String appHostname) throws Exception {

        String hostname = NetUtils.getHostname();
        if (hostname.equals(appHostname)) {
            return true;
        }

        String[] appHostnameParts = appHostname.split("/");
        String[] hostnameParts = hostname.split("/");

        return (compareFQDNs(appHostnameParts[0], hostnameParts[0]) && checkIPs(
                hostnameParts[0], hostnameParts[1], appHostnameParts[1]));
    }

    private boolean compareFQDNs(String appHostname, String hostname)
            throws Exception {
        if (appHostname.equals(hostname)) {
            return true;
        }
        String appFQDN = InetAddress.getByName(appHostname).getCanonicalHostName();
        String localFQDN = InetAddress.getByName(hostname).getCanonicalHostName();
        return appFQDN.equals(localFQDN);
    }

    private boolean checkIPs(String hostname, String localIP, String appIP)
            throws Exception {

        if (localIP.equals(appIP)) {
            return true;
        }
        boolean appIPCheck = false;
        boolean localIPCheck = false;
        InetAddress[] addresses = InetAddress.getAllByName(hostname);
        for (InetAddress ia : addresses) {
            if (ia.getHostAddress().equals(appIP)) {
                appIPCheck = true;
                continue;
            }
            if (ia.getHostAddress().equals(localIP)) {
                localIPCheck = true;
            }
        }
        return (appIPCheck && localIPCheck);

    }
}
