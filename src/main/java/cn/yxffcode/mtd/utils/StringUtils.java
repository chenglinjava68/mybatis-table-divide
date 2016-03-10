package cn.yxffcode.mtd.utils;

import java.util.ArrayList;

public class StringUtils {

  /**
   * 对字段进行trim如果字段原来就为null则仍然返回null
   *
   * @param str 目标字段
   * @return 为空时的提示信息
   */
  public static String trim(String str) {
    if (str != null) {
      return str.trim();
    }
    return null;
  }

  //String.split得到的每个子串没有去掉空格，另外如果有连续两个分隔符排在一起如"a,,b"则会得到一个""子串
  public static String[] split(String str, String delimiter) {
    String[] strs = str.split(delimiter);
    ArrayList<String> list = new ArrayList<String>(strs.length);

    for (String s : strs) {
      if (s != null && (s = s.trim()).length() > 0) {
        list.add(s);
      }
    }
    return list.toArray(new String[0]);
  }

}
