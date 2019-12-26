package cn.moretv.doraemon.biz.search;

import com.github.stuxuhai.jpinyin.PinyinFormat;
import com.github.stuxuhai.jpinyin.PinyinHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by cheng_huan on 2018/12/7.
 */
public class ObtainPinYinUtils {
    //判断字符串是否包含中文
    public static boolean isContainChinese(String string) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        if (string != null) {
            Matcher m = p.matcher(string);
            if (m.find()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 清除掉所有特殊字符，仅保留中文、数字、字母、空格
     *
     * @param string
     * @return
     */
    public static String removeSpecialCharacter(String string) {
        String regEx = "[^0-9a-zA-Z\\s\\u4e00-\\u9fa5]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(string);
        return m.replaceAll("").trim();
    }

    /**
     * 获取包含一个字符的拼音（多音字则以数组形式返回多个）,非汉字则返回字符本身
     */
    public static String[] getAllPinYins(String word) {
        String[] formatPinyin = null;
        String[] pinYin = null;
        try {
            if (isContainChinese(word)) {
                formatPinyin = PinyinHelper.convertToPinyinArray(word.charAt(0), PinyinFormat.WITHOUT_TONE);//获取对应的汉字拼音，不是汉字返回null
            } else {
                formatPinyin = new String[1];
                formatPinyin[0] = word;
            }
            pinYin = new String[formatPinyin.length];
            for (int i = 0; i < formatPinyin.length; i++) {
                pinYin[i] = formatPinyin[i].toLowerCase() + " ";
            }
        } catch (Exception e) {//会抛出异常，捕获异常
            e.printStackTrace();
        }
        return pinYin;
    }

    /**
     * 用递归方法，求出这个二维数组每行取一个数据组合起来的所有情况，返回一个字符串数组
     *
     * @param s      求组合数的2维数组
     * @param len    此二维数组的长度，省去每一次递归长度计算的时间和空间消耗，提高效率
     * @param cursor 类似JDBC、数据库、迭代器里的游标，指明当前从第几行开始计算求组合数，此处从0开始（数组第一行）
     *               避免在递归中不断复制剩余数组作为新参数，提高时间和空间的效率
     * @return String[] 以数组形式返回所有的组合情况
     */
    public static String[] recursionArrays(String[][] s, int len, int cursor) {
        if (cursor <= len - 2) {//递归条件,直至计算到还剩2行
            int len1 = s[cursor].length;
            int len2 = s[cursor + 1].length;
            int newLen = len1 * len2;//上下2行的总共的组合情况
            String[] temp = new String[newLen];//存上下2行中所有的组合情况
            int index = 0;
            for (int i = 0; i < len1; i++) {//嵌套循环遍历，求出上下2行中，分别取一个数据组合起来的所有情况
                for (int j = 0; j < len2; j++) {
                    temp[index] = s[cursor][i] + s[cursor + 1][j];
                    index++;
                }
            }
            s[cursor + 1] = temp;//把当前计算到此行的所有组合结果放在cursor+1行
            cursor++;//游标指向下一行，即上边的数据结果
            return ObtainPinYinUtils.recursionArrays(s, len, cursor);
        } else {
            return s[len - 1];//返回最终的所有组合结果
        }
    }

    /**
     * 获取拼音全拼(包含多音字)
     *
     * @param string 中文字符串
     * @return 拼音全拼数组
     */
    public static List<String> polyPinYinQuanPin(String string) {
        if (string == null) {
            string = "";//null时处理，后边处理时报错
        }
        String chines = removeSpecialCharacter(string);
        List<String> list = new ArrayList<>();
        if (chines.length() > 0) {
            String[][] allPinYins = new String[chines.length()][];//存放整个字符串的各个字符所有可能的拼音情况，如果非汉字则是它本身

            int i = 0;
            int j = 0;
            while (i < chines.length()) {
                StringBuilder keys = new StringBuilder();
                char c = chines.charAt(i);
                if (isContainChinese(String.valueOf(c))) {
                    //中文
                    try {
                        allPinYins[j] = ObtainPinYinUtils.getAllPinYins(String.valueOf(c));//每个字符的所有拼音情况
                    } catch (Exception e) {
                        System.out.println(String.valueOf(c) + "不能识别！");
                    }
                    i++;
                    j++;
                } else if ('0' <= c && '9' >= c) {
                    //数字
                    keys.append(String.valueOf(c));
                    //移动到数字的末尾
                    while (String.valueOf(c).matches("[0-9]") && i < chines.length()) {
                        i++;
                        if (i < chines.length()) {
                            c = chines.charAt(i);
                            if (String.valueOf(c).matches("[0-9]")) {
                                keys.append(c);
                            }
                        }
                    }
                    allPinYins[j] = ObtainPinYinUtils.getAllPinYins(keys.toString());
//                i++;
                    j++;
                } else if ('A' <= c && 'Z' >= c || 'a' <= c && 'z' >= c) {
                    //英文
                    keys.append(String.valueOf(c));
                    while (String.valueOf(c).matches("[a-zA-Z]") && i < chines.length()) {
                        i++;
                        if (i < chines.length()) {
                            c = chines.charAt(i);
                            if (String.valueOf(c).matches("[a-zA-Z]")) {
                                keys.append(String.valueOf(c));
                            }
                        }
                    }
                    while (i < chines.length() && c == ' ') {
                        i++;
                        if (i < chines.length()) {
                            c = chines.charAt(i);
                        }
                    }
                    allPinYins[j] = ObtainPinYinUtils.getAllPinYins(keys.toString());
                    j++;
                } else {
                    i++;
                }
            }
            if (allPinYins[0] != null) {
                String[] resultArray = ObtainPinYinUtils.recursionArrays(allPinYins, j, 0);//用递归，得到这个2维数组每行取一个数据组合起来的所有情况
                list = Arrays.asList(resultArray);//返回数组支持的固定大小的list来实现对结果随意操作
            }
        }
        return list;
    }
}
