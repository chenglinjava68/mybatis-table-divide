package cn.yxffcode.mtd.core.parser.ast;

import cn.yxffcode.mtd.utils.CollectionUtils;
import com.google.common.collect.Sets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author gaohang on 16/3/10.
 */
public class UpdateStatement implements SqlStatement {
  private List<Table> tables = Collections.emptyList();
  private Set<String> tableNames = Collections.emptySet();
  private List<Column> columns = Collections.emptyList();
  private List<Expression> expressions = Collections.emptyList();
  private Expression where;

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

  public List<Column> getColumns() {
    return columns;
  }

  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  public List<Expression> getExpressions() {
    return expressions;
  }

  public void setExpressions(List<Expression> expressions) {
    this.expressions = expressions;
  }

  public Expression getWhere() {
    return where;
  }

  public void setWhere(Expression where) {
    this.where = where;
  }

  @Override public Set<String> getTableNames() {
    return tableNames;
  }
}
