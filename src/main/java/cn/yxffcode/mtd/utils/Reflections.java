package cn.yxffcode.mtd.utils;

import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * @author gaohang on 16/1/3.
 */
public final class Reflections {
  private Reflections() {
  }

  public static Object getField(String fieldName, Object target) {
    Field field = ReflectionUtils.findField(target.getClass(), fieldName);
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }

    // FIXME: 16/1/3 whether to set accessible to false
    return ReflectionUtils.getField(field, target);
  }

  public static boolean hasField(String fieldName, Class<?> c) {
    Field field = ReflectionUtils.findField(c, fieldName);
    return field != null;
  }
}
