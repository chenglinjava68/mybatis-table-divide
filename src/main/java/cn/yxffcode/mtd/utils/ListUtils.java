package cn.yxffcode.mtd.utils;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaohang on 15/9/29.
 */
public final class ListUtils {
  private ListUtils() {
  }

  public static <E> List<? extends E> subList(List<? extends E> src, int offset, int length) {
    checkNotNull(src);
    if ((offset == 0 && src.size() <= length) || src.size() == 0) {
      return src;
    }

    //需要检查length和offset的值,防止IndexOutOfBoundsException异常
    if (offset >= src.size()) {
      return Collections.emptyList();
    }
    if (length + offset >= src.size()) {
      return src.subList(offset, src.size());
    }
    return src.subList(offset, length + offset);
  }

  public static <E> List<E> subListCopy(List<? extends E> src, int offset, int length) {
    List<? extends E> elements = subList(src, offset, length);
    ArrayList<E> r = new ArrayList<>(elements.size());
    for (E element : elements) {
      r.add(element);
    }
    return r;
  }

  public static <K, V> Multimap<K, V> partition(List<? extends V> data,
                                                Function<? super V, K> keyFunction) {
    checkNotNull(data);
    checkNotNull(keyFunction);
    if (Iterables.isEmpty(data)) {
      return ImmutableMultimap.of();
    }
    Multimap<K, V> result = HashMultimap.create();
    for (V v : data) {
      result.put(keyFunction.apply(v), v);
    }
    return result;
  }
}
