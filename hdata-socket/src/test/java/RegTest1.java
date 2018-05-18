import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegTest1 {


    public static void main(String[] args) {

        Pattern reg = Pattern.compile("(\\d+),(\\d+)");
        Matcher matcher = reg.matcher("11111,2222");
        if(matcher.find()){
            System.out.println(matcher.group(0));
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
        }
//        String[] arr = reg.split("11111,2222");
//
//        for(String s: arr){
//            System.out.println(s);
//        }
    }
}
