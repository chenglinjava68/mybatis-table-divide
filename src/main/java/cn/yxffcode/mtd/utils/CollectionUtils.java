package cn.yxffcode.mtd.utils;

import java.util.Collection;

/**
 * @author gaohang on 16/3/9.
 */
public final class CollectionUtils {
  private CollectionUtils() {
  }

  public static <E> boolean isNotEmpty(Collection<E> collection) {
    return collection != null && collection.size() != 0;
  }

  public static <E> boolean isEmpty(Collection<E> collection) {
    return !isNotEmpty(collection);
  }
}
