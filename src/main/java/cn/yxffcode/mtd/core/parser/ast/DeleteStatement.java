package cn.yxffcode.mtd.core.parser.ast;

import com.google.common.collect.Sets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.util.Collections;
import java.util.Set;

/**
 * @author gaohang on 16/3/10.
 */
public class DeleteStatement implements SqlStatement {
  private Table table;
  private Set<String> tableNames = Collections.emptySet();
  private Expression where;

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
