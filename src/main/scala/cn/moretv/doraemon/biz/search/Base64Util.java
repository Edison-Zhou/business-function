package cn.moretv.doraemon.biz.search;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;
/**
 * Created by cheng_huan on 2018/12/4.
 */
public class Base64Util {
    //base64编码
    public static String base64Encode(String originKeyword) {
        String result = null;
        if (originKeyword != null && !originKeyword.equals("")) {
            BASE64Encoder encoder = new BASE64Encoder();
            try {
                result = encoder.encode(originKeyword.getBytes()).replaceAll("\r|\n", "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    //base64解码
    public static String base64Decode(String originKeyword) {
        byte[] b = null;
        String result = null;
        if (originKeyword != null && !originKeyword.equals("")) {
            BASE64Decoder decoder = new BASE64Decoder();
            try {
                b = decoder.decodeBuffer(originKeyword);
                result = new String(b, "utf-8");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
