package cn.yxffcode.mtd.core.merger;

import cn.yxffcode.mtd.core.FieldMapping;
import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.parser.ast.SelectStatement;
import cn.yxffcode.mtd.utils.ListUtils;
import cn.yxffcode.mtd.utils.Reflections;
import com.google.common.base.Supplier;
import com.google.common.collect.Ordering;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 查询结果的合并逻辑
 *
 * @author gaohang on 16/2/2.
 */
enum Mergers {
  AVG {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           ParameterSupplier parameterSupplier) throws SQLException {
      throw new SQLException("The group function 'AVG' is not supported now!");
    }
  },

  COUNT {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           ParameterSupplier parameterSupplier) throws SQLException {
      GroupList objects = new GroupList(results);
      if (objects.size <= 1) {
        return objects;
      }
      long sum = 0;
      for (Object result : objects) {
        if (result == null) {
          continue;
        }
        if (result instanceof Long) {
          sum += ((Long) result);
        } else if (result instanceof Integer) {
          sum += ((Integer) result);
        }
      }
      return Arrays.asList(sum);
    }
  },
  MAX {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           ParameterSupplier parameterSupplier) throws SQLException {
      GroupList objects = new GroupList(results);
      if (objects.size <= 1) {
        return objects;
      }
      Comparable<Object> max = null;
      for (Object current : objects) {
        if (current == null) {
          continue;
        }
        if (max == null || max.compareTo(current) < 0) {
          max = (Comparable<Object>) current;
        }
      }
      return Arrays.asList(max);
    }
  },
  MIN {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           ParameterSupplier parameterSupplier) throws SQLException {
      GroupList objects = new GroupList(results);
      if (objects.size <= 1) {
        return objects;
      }
      Comparable<Object> min = null;
      for (Object current : objects) {
        if (current == null) {
          continue;
        }
        if (min == null || min.compareTo(current) > 0) {
          min = (Comparable<Object>) current;
        }
      }
      return Arrays.asList(min);
    }
  },
  SUM {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           ParameterSupplier parameterSupplier) throws SQLException {
      Adders.Add<Object> add = null;
      Object sum = null;
      GroupList objects = new GroupList(results);
      if (objects.size <= 1) {
        return objects;
      }
      for (Object result : objects) {
        if (result == null) {
          continue;
        }
        if (sum == null) {
          sum = result;
          add = Adders.getNumberAdd(sum.getClass());
          checkNotNull(add, "The group function 'SUM' does not supported the type '%s'",
              sum.getClass());
        } else {
          sum = add.add(sum, result);
        }
      }
      return Arrays.asList(sum);
    }
  },
  DISTINCT {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           ParameterSupplier parameterSupplier) throws SQLException {
      // TODO: 16/2/2 没有处理Distinct
      return new GroupList(results);
    }
  },
  ORDER_BY {
    @Override Object merge(List<Object> results, final ParsedSqlContext parsedSqlContext,
                           final Supplier<FieldMapping> fieldMappingSupplier,
                           final ParameterSupplier parameterSupplier) throws SQLException {
      Ordering<Object> ordering = buildOrdering(parsedSqlContext, fieldMappingSupplier);

      SelectStatement selectStatement = (SelectStatement) parsedSqlContext.getSqlStatement();

      Limit limit = selectStatement.getLimit();
      if (limit == null) {
        //不需要limit
        return ordering.sortedCopy(new GroupList(results));
      }

      //offset和limit是最后两个参数
      int parameterCount = parameterSupplier.getParameterCount();
      int off = limit.isOffsetJdbcParameter() ?
          (Integer) parameterSupplier.getParameter(parameterCount - 2) :
          (int) limit.getOffset();

      if (off < 0) {
        off = 0;
      }
      int len = limit.isRowCountJdbcParameter() ?
          (Integer) parameterSupplier.getParameter(parameterCount - 1) :
          (int) limit.getRowCount();
      List<Object> sorted = ordering.sortedCopy(new GroupList(results));
      return ListUtils.subListCopy(sorted, off, len);
    }

    private Ordering<Object> buildOrdering(final ParsedSqlContext parsedSqlContext,
                                           final Supplier<FieldMapping> fieldMappingSupplier) {

      final SelectStatement selectStatement = (SelectStatement) parsedSqlContext.getSqlStatement();
      return new Ordering<Object>() {
        private List<OrderByElement> orderByEles = selectStatement.getOrderByElements();
        private FieldMapping fieldMapping = fieldMappingSupplier.get();

        @Override public int compare(Object left, Object right) {
          //依次比较每一个order by子句
          for (int i = 0, j = orderByEles.size(); i < j; i++) {
            OrderByElement orderByEle = orderByEles.get(i);
            if (orderByEle == null) {
              continue;
            }
            String orderByColumnName = getOrderByColumnName(orderByEle, selectStatement);
            //取值,这里比较麻烦,需要处理left不是复杂对象的情况,如果是基于jdbc,则相对容易
            Comparable<Object> lv;
            if (Reflections.hasField(fieldMapping.map(orderByColumnName), left.getClass())) {
              lv = (Comparable<Object>) Reflections
                  .getField(fieldMapping.map(orderByColumnName), left);
            } else {
              lv = (Comparable<Object>) left;//直接转型,这里忽略了属性不存在的情况,如果属性不存在,无法做排序,转型会出错
            }

            Object rv =
                Reflections.hasField(fieldMapping.map(orderByColumnName), right.getClass()) ?
                    Reflections.getField(fieldMapping.map(orderByColumnName), right) :
                    (Comparable<Object>) right;

            //比较大小
            int cmp = lv.compareTo(rv);
            if (cmp == 0) {//相等,则比较下一个字段
              continue;
            }
            return orderByEle.isAsc() ? cmp : -cmp;
          }
          return 0;
        }
      };
    }
  },
  DEFAULT {
    @Override Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
                           Supplier<FieldMapping> fieldMappingSupplier,
                           final ParameterSupplier parameterSupplier) throws SQLException {
      SelectStatement selectStatement = (SelectStatement) parsedSqlContext.getSqlStatement();
      //不排序,但处理limit
      Limit limit = selectStatement.getLimit();
      if (limit == null) {
        return new GroupList(results);
      }

      int len = (int) limit.getRowCount();
      int off = (int) limit.getOffset();
      return ListUtils.subListCopy(new GroupList(results), off, len);
    }
  };

  private static String getOrderByColumnName(OrderByElement orderByEle,
                                             SelectStatement selectStatement) {
    Column orderByColumn = (Column) orderByEle.getExpression();
    String orderByColumnName = orderByColumn.getColumnName();
    Table orderByTable = orderByColumn.getTable();
    //如果不同表中有相同字段,取出order by的字段的别名
    List<SelectItem> selectItems = selectStatement.getSelectItems();
    for (SelectItem selectItem : selectItems) {
      if (selectItem instanceof AllColumns || selectItem instanceof AllTableColumns) {
        //无法排序
        throw new ResultMergeException(
            "select * statement cannot merge results through order by");
      }
      SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
      Expression expression = selectExpressionItem.getExpression();

      //因为查询到结果后,mybatis会通过别名做映射,所以如果别名存在,应该将列名转换为别名
      Alias alias = selectExpressionItem.getAlias();
      String aliasName = alias == null ? StringUtils.EMPTY : alias.getName();
      if (!(expression instanceof Column)) {
        continue;
      }
      Column selectColumn = (Column) expression;
      String selectColumnName = selectColumn.getColumnName();
      if (StringUtils.equals(orderByColumnName, selectColumnName)) {
        Table selectColumnTable = selectColumn.getTable();
        //别名相等时,结束查找,别名不等时,继续找是否有别名同时也匹配的
        orderByColumnName = alias.getName();
        if (selectColumnTable != null && orderByTable != null &&
            StringUtils.equals(selectColumnTable.getName(), orderByTable.getName())) {
          break;
        }
      }
    }
    return orderByColumnName;
  }

  abstract Object merge(final List<Object> results, final ParsedSqlContext parsedSqlContext,
                        final Supplier<FieldMapping> fieldMappingSupplier,
                        final ParameterSupplier parameterSupplier) throws SQLException;


  /**
   * a group of list.using AbstractList to avoid O(n) time waste.
   */
  private static final class GroupList extends AbstractList<Object> {

    private List<List<Object>> lists;
    private int size;

    private GroupList(List<Object> lists) {
      this.lists = new ArrayList<>(lists.size());
      for (Object obj : lists) {
        @SuppressWarnings("unchecked") List<Object> list = (List<Object>) obj;
        size += list.size();
        this.lists.add(list);
      }
    }

    @Override public Object get(int index) {
      checkArgument(index >= 0 && index < size);
      //如果子集合比较多，可以换成二分查找
      int remaining = index;
      for (List<Object> list : lists) {
        if (list.size() > remaining) {
          return list.get(remaining);
        }
        remaining -= list.size();
      }
      return null;
    }

    @Override public int size() {
      return size;
    }
  }

}
