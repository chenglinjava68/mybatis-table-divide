package cn.yxffcode.mtd.core.router;

import cn.yxffcode.mtd.lang.ImmutableIterator;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author gaohang on 15/12/29.
 */
public enum Routers {
  /**
   * mod(id, 5)
   */
  MOD("mod") {
    @Override public Router generateRouter(String paramString) {
      List<String> params = comaSplitter.splitToList(paramString);
      checkState(params.size() == 2);
      final int mod = Integer.parseInt(params.get(1));
      return new AbstractRouter(params.get(0)) {
        @Override protected Set<String> allSubNames() {
          return new AbstractSet<String>() {
            @Override public Iterator<String> iterator() {
              return new ImmutableIterator<String>() {
                private int i = 0;

                @Override public boolean hasNext() {
                  return i < mod;
                }

                @Override public String next() {
                  return Integer.toString(i++);
                }
              };
            }

            @Override public int size() {
              return mod;
            }
          };
        }

        @Override protected void doWithColumnValue(Object value, Set<String> suffixes) {
          if (value instanceof Integer) {
            suffixes.add(Integer.toString((Integer) value % mod));
          } else if (value instanceof Long) {
            suffixes.add(Long.toString((Long) value % mod));
          } else {
            String v = value.toString();
            suffixes.add(Integer.toString(Integer.parseInt(v) % mod));
          }
        }
      };
    }
  },
  /**
   * month(date)
   */
  MONTH("month") {
    private final Set<String> ALL = Sets.newHashSet("1 2 3 4 5 6 7 8 9 10 11 12".split(" "));

    @Override public Router generateRouter(String paramString) {
      return new AbstractRouter(paramString.trim()) {
        private FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd");

        @Override protected Set<String> allSubNames() {
          return ALL;
        }

        @Override protected void doWithColumnValue(Object value, Set<String> suffixes) {
          if (value instanceof Date) {
            suffixes.add(Integer.toString(((Date) value).getMonth() + 1));
          } else {
            try {
              Date date = fdf.parse(value.toString());
              suffixes.add(Integer.toString(date.getMonth()));
            } catch (ParseException e) {
              Throwables.propagate(e);
            }
          }
        }

      };
    }
  },
  /**
   * val(col)
   */
  VALUE("val") {
    @Override public Router generateRouter(String paramString) {
      return new AbstractRouter(paramString.trim()) {
        @Override protected Set<String> allSubNames() {
          return Collections.emptySet();
        }

        @Override protected void doWithColumnValue(Object value, Set<String> suffixes) {
          suffixes.add(value.toString());
        }

      };
    }
  };
  protected final Splitter comaSplitter = Splitter.on(',').trimResults();
  private final String func;

  Routers(String func) {
    this.func = func;
  }

  public String func() {
    return func;
  }

  /**
   * @param paramString String from xml.For example, router strategy is mod(id, 5), the paramString
   *                    is 'id, 5'
   */
  public abstract Router generateRouter(String paramString);
}
