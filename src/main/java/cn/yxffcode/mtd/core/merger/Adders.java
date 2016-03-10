package cn.yxffcode.mtd.core.merger;

import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

/**
 * 各种数据类型的加法器实现工具类,用作查询结果的合并
 *
 * @author gaohang
 */
public class Adders {
  private static final Map<Class<?>, Add<Object>> ADDERS;

  static {
    Map<Class<?>, Add<Object>> tmp = Maps.newHashMap();
    tmp.put(Integer.class, AddImpl.INTEGER);
    tmp.put(Float.class, AddImpl.FLOAT);
    tmp.put(Long.class, AddImpl.LONG);
    tmp.put(Double.class, AddImpl.DOUBLE);
    tmp.put(BigDecimal.class, AddImpl.BIG_DECIMAL);
    tmp.put(Short.class, AddImpl.SHORT);
    tmp.put(Byte.class, AddImpl.BYTE);
    ADDERS = Collections.unmodifiableMap(tmp);
  }

  public static Add<Object> getNumberAdd(Class<?> c) {
    return ADDERS.get(c);
  }

  private enum AddImpl implements Add<Object> {
    INTEGER {
      @Override public Object add(Object left, Object right) {
        return (Integer) left + (Integer) right;
      }
    },
    FLOAT {
      @Override public Object add(Object left, Object right) {
        return (Float) left + (Float) right;
      }
    },
    LONG {
      @Override public Object add(Object left, Object right) {
        return (Long) left + (Long) right;
      }
    },
    DOUBLE {
      @Override public Object add(Object left, Object right) {
        return (Double) left + (Double) right;
      }
    },
    BIG_DECIMAL {
      @Override public Object add(Object left, Object right) {
        return ((BigDecimal) left).add((BigDecimal) right);
      }
    },
    SHORT {
      @Override public Object add(Object left, Object right) {
        return (Short) left + (Short) right;
      }
    },
    BYTE {
      @Override public Object add(Object left, Object right) {
        return (Byte) left + (Byte) right;
      }
    }
  }


  public interface Add<T> {
    T add(T left, T right);
  }

}
