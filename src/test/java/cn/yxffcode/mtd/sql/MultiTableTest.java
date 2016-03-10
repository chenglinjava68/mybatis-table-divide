package cn.yxffcode.mtd.sql;

import cn.yxffcode.mtd.core.FieldMapping;
import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.merger.ResultMergerImpl;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.parser.SQLParserImpl;
import cn.yxffcode.mtd.core.parser.ast.SqlStatement;
import cn.yxffcode.mtd.core.rewriter.SqlRewriterImpl;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * @author gaohang on 16/3/1.
 */
public class MultiTableTest {

  @Test
  public void testRewrite() {
    String sql = "select a.i as id, name from crawl_auto a where a.i in (1, 2)";
    SqlStatement sqlStatement = SQLParserImpl.getInstance().parse(sql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, sqlStatement);

    Iterator<CharSequence> sqls =
        SqlRewriterImpl.getInstance().rewrite(parsedSqlContext, new ParameterSupplier() {
          @Override public int getParameterCount() {
            return 0;
          }

          @Override public Object getParameter(int parameterIndex) {
            return null;
          }

          @Override public Object getParameter(String propertyName) {
            return null;
          }
        });
    for (; sqls.hasNext(); ) {
      System.out.println(sqls.next());
    }
  }

  @Test
  public void testShard() throws SQLException {
    String sql = "select a.i as id, name from table_a a order by i desc limit ?, ?";
    SqlStatement sqlStatement = SQLParserImpl.getInstance().parse(sql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, sqlStatement);

    ResultMergerImpl merger = ResultMergerImpl.getInstance();

    List<Object> objects = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      List<Bean> beans = Lists.newArrayList();
      for (int j = 0; j < 20; j++) {
        Bean bean = new Bean();
        bean.id = i * 20 + j;
        bean.name = "i" + bean.id;
        beans.add(bean);
      }
      objects.add(beans);
    }

    Object merge =
        merger.merge(objects, parsedSqlContext, new Supplier<FieldMapping>() {
          @Override public FieldMapping get() {
            return new FieldMapping() {
              @Override public String map(String col) {
                return col;
              }
            };
          }
        }, new ParameterSupplier() {
          @Override public int getParameterCount() {
            return 2;
          }

          @Override public Object getParameter(int parameterIndex) {
            if (parameterIndex == 0) {
              return 40;
            }
            return 10;
          }

          @Override public Object getParameter(String propertyName) {
            return null;
          }
        });
    System.out.println(merge);
  }

  public static class Bean {
    private int id;
    private String name;

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override public String toString() {
      return "Bean{" +
          "id=" + id +
          ", name='" + name + '\'' +
          '}';
    }
  }
}
