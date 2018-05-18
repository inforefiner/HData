import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegTest {


    public static void main(String[] args) {

        String str = "<129>2018-03-07 06:43:53 log_type=event;send_time=2018-03-07  06:50:01+08:00;event_id=8909709462890963418;src_ip=139.199.110.78;dst_ip=221.179.140.189;event_type=UDP Flood;src_region=未知业务;dst_region=未知业务;src_isp=其他-CGWNET;dst_isp=中国移动-CMNET;src_location=中国广东深圳;dst_location=中国北京;total_bytes=990000;total_packets=15000;max_bps=264000;max_pps=500;start_time=2018-03-07 06:39:32+08:00;end_time=2018-03-07 06:40:48+08:00 sendHostIp=111.13.158.184";
//        String str1 = "(<id>(\\d*)) (<name>(\\w*)) (<age>(\\d*))";

        Pattern reg = Pattern.compile("(?<featureValues>(\\d+))>(?<timestamp>([\\d-]+\\s+[\\d:]+))\\s+log_type=(?<logtype>(\\S+));send_time=(?<sendtime>([\\d-]+\\s+[\\d:]+))(?<sendtimezone>(\\+[\\d:]+));event_id=(?<eventid>(\\d+));src_ip=(?<srcIP>([\\d.-])+);dst_ip=(?<dstIP>([\\d.-])+);event_type=(?<eventtype>([\\s\\S]+));src_region=(?<srcregion>(\\S+));dst_region=(?<dstregion>(\\S+));src_isp=(?<srcisp>(\\S+));dst_isp=(?<dstisp>(\\S+));src_location=(?<srclocation>(\\S+));dst_location=(?<dstlocation>(\\S+));total_bytes=(?<totalbytes>(\\d+));total_packets=(?<totalpackets>(\\d+));max_bps=(?<maxbps>(\\d+));max_pps=(?<maxpps>(\\d+));start_time=(?<starttime>([\\d-]+\\s+[\\d:]+))(?<startimezone>(\\+[\\d:]+));end_time=(?<endtime>([\\d-]+\\s+[\\d:]+))(?<endtimezone>(\\+[\\d:]+))\\s+sendHostIp=(?<hostIP>([\\d.-])+)");

        Matcher matcher = reg.matcher(str);
        if (matcher.find()) {
            System.out.println(matcher.group("featureValues"));
            System.out.println(matcher.group("timestamp"));
            System.out.println(matcher.group("logtype"));
//            for (int i = 1; i <= matcher.groupCount(); i++) {
//                System.out.println(matcher.group(i));
//            }
        }
//        for(int i = 1; i < matcher.groupCount(); i++){
//            System.out.println(matcher.group(i));
//        }
//        System.out.println(matcher.groupCount());
    }
}
