import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.ArrayType;

import java.util.Date;

public class BlobTest {

    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
//        objectMapper.enable(SerializationFeature.);
    }

    public static void main(String[] args) {

        Object[] arr = new Object[7];
        arr[0] = 1;
        arr[1] = "test";
        arr[2] = new Date();
        arr[3] = 1;
        arr[4] = "test";
        arr[5] = new Date();
        arr[6] = null;

        try {
            byte[] row = objectMapper.writeValueAsBytes(arr);

//            Object objects = objectMapper.readValue

            System.out.println(row.length);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
