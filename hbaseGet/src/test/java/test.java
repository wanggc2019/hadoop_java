import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class test {
    public static void main(String[] args) throws DecoderException {
        String key = "000003611c05b536356afe95f7a57bffc9e9cfef";
        System.out.println("key=" + key);
        System.out.println("char array= " + key.toCharArray());
        System.out.println("hex=" + Hex.decodeHex(key.toCharArray()));


        System.out.println(Bytes.toBytes(key));

    }
}
