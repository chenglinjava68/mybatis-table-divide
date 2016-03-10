package cn.yxffcode.mtd.core.parser.ast;

import cn.yxffcode.mtd.utils.CollectionUtils;
import com.google.common.collect.Sets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author gaohang on 16/3/9.
 */
public class SelectStatement implements SqlStatement {
  public static final SelectStatement EMPTY = new SelectStatement();
  private List<Table> tables = Collections.emptyList();
  private Set<String> tableNames = Collections.emptySet();
  private Limit limit;
  private List<OrderByElement> orderByElements = Collections.emptyList();
  private Expression where;
  private List<Column> groupByColumnReferences = Collections.emptyList();
  private List<SelectItem> selectItems = Collections.emptyList();
  private GroupFunctionType groupFuncType = GroupFunctionType.NONE;
  private Distinct distinct;

  public List<Table> getTables() {
    return tables;
  }

  public void setTables(List<Table> tables) {
    this.tables = tables;
    if (CollectionUtils.isNotEmpty(tables)) {
      tableNames = Sets.newHashSetWithExpectedSize(tables.size());
      for (Table table : tables) {
        tableNames.add(table.getName());
      }
    } else {
      tableNames = Collections.emptySet();
    }
  }

  public Limit getLimit() {
    return limit;
  }

  public void setLimit(Limit limit) {
    this.limit = limit;
  }

  public List<OrderByElement> getOrderByElements() {
    return orderByElements;
  }

  public void setOrderByElements(List<OrderByElement> orderByElements) {
    this.orderByElements = orderByElements;
  }

  public Expression getWhere() {
    return where;
  }

  public void setWhere(Expression where) {
    this.where = where;
  }

  public List<Column> getGroupByColumnReferences() {
    return groupByColumnReferences;
  }

  public void setGroupByColumnReferences(
      List<Column> groupByColumnReferences) {
    this.groupByColumnReferences = groupByColumnReferences;
  }

  public GroupFunctionType getGroupFuncType() {
    return groupFuncType;
  }

  public void setGroupFuncType(GroupFunctionType groupFuncType) {
    this.groupFuncType = groupFuncType;
  }

  public Distinct getDistinct() {
    return distinct;
  }

  public void setDistinct(Distinct distinct) {
    this.distinct = distinct;
  }

  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

  public void setSelectItems(List<SelectItem> selectItems) {
    this.selectItems = selectItems;
  }

  @Override public Set<String> getTableNames() {
    return tableNames;
  }
}
