package cn.yxffcode.mtd.core.parser.ast;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;

import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

import static cn.yxffcode.mtd.utils.CollectionUtils.isNotEmpty;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaohang on 16/3/9.
 */
public final class StatementUtils {
  private StatementUtils() {
  }

  public static SqlStatement parseStatement(Statement statement) {
    if (statement instanceof Select) {
      return parseSelect((Select) statement);
    }
    if (statement instanceof Update) {
      return parseUpdate((Update) statement);
    }
    if (statement instanceof Delete) {
      return parseDelete((Delete) statement);
    }
    if (statement instanceof InsertStatement) {
      return parseInsert((Insert) statement);
    }
    throw new UnsupportedStatementException("Only select, update and delete statements are "
        + "supported, unsupported sql is " + statement);
  }

  public static InsertStatement parseInsert(Insert insert) {
    checkNotNull(insert);
    InsertStatement insertStatement = new InsertStatement();
    insertStatement.setColumns(ImmutableList.copyOf(insert.getColumns()));
    insertStatement.setTable(insert.getTable());
    insertStatement.setItemsList(insert.getItemsList());
    if (insert.getSelect() != null) {
      insertStatement.setSelectStatement(parseSelect(insert.getSelect()));
    }
    return insertStatement;
  }

  public static SelectStatement parseSelect(Select select) {
    checkNotNull(select);
    SelectBody selectBody = select.getSelectBody();
    return parseSelectBody(selectBody);
  }

  public static SelectStatement parseSelectBody(final SelectBody selectBody) {
    if (selectBody == null) {
      return SelectStatement.EMPTY;
    }
    final SelectStatement selectStatement = new SelectStatement();
    selectBody.accept(new SelectVisitorAdapter() {
      @Override public void visit(PlainSelect plainSelect) {

        GroupFunctionType groupFuncType = parseGroupFuncType(plainSelect, selectBody);
        if (groupFuncType != null) {
          selectStatement.setGroupFuncType(groupFuncType);
        }

        List<Table> tables = parseTables(plainSelect);
        if (tables != null) {
          selectStatement.setTables(tables);
        }

        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        if (isNotEmpty(orderByElements)) {
          selectStatement.setOrderByElements(ImmutableList.copyOf(orderByElements));
        }
        Limit limit = plainSelect.getLimit();
        if (limit != null) {
          selectStatement.setLimit(limit);
        }

        //where,可能是简单条件或者树结构
        Expression where = plainSelect.getWhere();
        if (where != null) {
          selectStatement.setWhere(where);
        }

        Distinct distinct = plainSelect.getDistinct();
        if (distinct != null) {
          selectStatement.setDistinct(distinct);
        }

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        if (isNotEmpty(selectItems)) {
          selectStatement.setSelectItems(selectItems);
        }

        final List<Expression> groupByColumnReferences = plainSelect.getGroupByColumnReferences();
        if (isNotEmpty(groupByColumnReferences)) {
          selectStatement.setGroupByColumnReferences(new AbstractList<Column>() {
            @Override public Column get(int index) {
              return (Column) groupByColumnReferences.get(index);
            }

            @Override public int size() {
              return groupByColumnReferences.size();
            }
          });
        }
      }
    });
    return selectStatement;
  }

  private static GroupFunctionType parseGroupFuncType(PlainSelect plainSelect,
                                                      SelectBody selectBody) {
    List<SelectItem> selectItems = plainSelect.getSelectItems();
    if (isNotEmpty(selectItems) && selectItems.size() == 1) {
      SelectItem selectItem = selectItems.get(0);
      if (selectItem instanceof SelectExpressionItem) {
        Expression expression = ((SelectExpressionItem) selectItem).getExpression();
        if (expression instanceof Function) {
          String functionName = ((Function) expression).getName().toUpperCase();
          try {
            return GroupFunctionType.valueOf(functionName);
          } catch (IllegalArgumentException e) {
            throw new UnsupportedStatementException("unknown function:" + functionName + " "
                + "sql:" + selectBody.toString());
          }
        }
      }
    }
    return null;
  }

  public static UpdateStatement parseUpdate(Update update) {
    checkNotNull(update);
    UpdateStatement updateStatement = new UpdateStatement();
    List<Table> tables = update.getTables();
    if (isNotEmpty(tables)) {
      updateStatement.setTables(ImmutableList.copyOf(tables));
    }
    List<Expression> expressions = update.getExpressions();
    if (isNotEmpty(expressions)) {
      updateStatement.setExpressions(ImmutableList.copyOf(expressions));
    }
    Expression where = update.getWhere();
    if (where != null) {
      updateStatement.setWhere(where);
    }
    return updateStatement;
  }

  public static DeleteStatement parseDelete(Delete delete) {
    DeleteStatement deleteStatement = new DeleteStatement();
    deleteStatement.setTable(delete.getTable());
    deleteStatement.setWhere(delete.getWhere());
    return deleteStatement;
  }

  private static List<Table> parseTables(PlainSelect plainSelect) {
    List<Table> tables = null;
    FromItem fromItem = plainSelect.getFromItem();
    if (fromItem instanceof Table) {
      tables = Lists.newArrayList();
      tables.add((Table) fromItem);
    }

    //join
    List<Join> joins = plainSelect.getJoins();
    if (isNotEmpty(joins)) {
      for (Join join : joins) {
        FromItem rightItem = join.getRightItem();
        if (rightItem == null) {
          continue;
        }
        if (rightItem instanceof Table) {
          if (tables == null) {
            tables = Lists.newArrayList();
          }
          tables.add((Table) rightItem);
        }
      }
    }
    return tables == null ? null : Collections.unmodifiableList(tables);
  }
}
