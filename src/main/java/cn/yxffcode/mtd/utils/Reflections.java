package cn.yxffcode.mtd.utils;

import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * @author gaohang on 16/1/3.
 */
public final class Reflections {
  private Reflections() {
  }

  public static void setField(Object target, String field, Object value) {
    Field ss = findField(target.getClass(), field);
    ss.setAccessible(true);
    setField(ss, target, value);
  }

  public static void setField(Field field, Object target, Object value) {
    try {
      field.set(target, value);
    } catch (IllegalAccessException ex) {
      throw new IllegalStateException(
          "Unexpected reflection exception - " + ex.getClass().getName() + ": " + ex.getMessage()
          , ex);
    }
  }

  private static Field findField(Class<?> clazz, String name) {
    return findField(clazz, name, null);
  }
  public static Field findField(Class<?> clazz, String name, Class<?> type) {
    Class<?> searchType = clazz;
    while (!Object.class.equals(searchType) && searchType != null) {
      Field[] fields = searchType.getDeclaredFields();
      for (Field field : fields) {
        if ((name == null || name.equals(field.getName())) && (type == null || type
            .equals(field.getType()))) {
          return field;
        }
      }
      searchType = searchType.getSuperclass();
    }
    return null;
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
