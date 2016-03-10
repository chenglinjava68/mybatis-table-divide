package cn.yxffcode.mtd.core.parser.ast;

import com.google.common.collect.Sets;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author gaohang on 16/3/10.
 */
public class InsertStatement implements SqlStatement {
  private Table table;
  private Set<String> tableNames = Collections.emptySet();
  private List<Column> columns = Collections.emptyList();
  private ItemsList itemsList;
  private SelectStatement selectStatement;

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
    if (table == null) {
      tableNames = Collections.emptySet();
    } else {
      tableNames = Sets.newHashSetWithExpectedSize(1);
      tableNames.add(table.getName());
    }
  }

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public ItemsList getItemsList() {
    return itemsList;
  }

  public void setItemsList(ItemsList itemsList) {
    this.itemsList = itemsList;
  }

  public SelectStatement getSelectStatement() {
    return selectStatement;
  }

  public void setSelectStatement(SelectStatement selectStatement) {
    this.selectStatement = selectStatement;
  }

  @Override public Set<String> getTableNames() {
    return tableNames;
  }
}
