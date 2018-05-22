import com.github.stuxuhai.hdata.plugin.reader.socket.utils.JsonBuilder;

import java.util.HashMap;
import java.util.Map;

public class JsonTest {

    public static void main(String[] args) {

//        System.out.println(args[0]);
//        String str = "{\"a1\":\"(?<id>(\\\\d*))%20(?<name>(\\\\w*))\"}";
//        String str = args[0];
//        String str1 = StringEscapeUtils.escapeJava(str);
//        System.out.println(str1);
//        Map map = JsonBuilder.getInstance().fromJson(str, HashMap.class);
//
//        System.out.println(map);
//        String reg = "(?<id>(\\d*)) (?<name>(\\w*))";
//        String reg2 = "(?<id>(\\d*)) (?<age>(\\d*))";
//        Pattern p = Pattern.compile(reg);
//        System.out.println(p.matcher("10 ttt").find());
        String str = "{\"k1\":{\"kk1\":\"v1\",\"kk2\":\"v2\"},\"k2\":{\"ff1\":\"v2\",\"ff2\":\"v2\"}}";
        Map map = JsonBuilder.getInstance().fromJson(str, HashMap.class);
        System.out.println(map);
    }
}
