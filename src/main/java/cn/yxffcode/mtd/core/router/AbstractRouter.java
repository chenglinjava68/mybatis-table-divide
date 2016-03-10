package cn.yxffcode.mtd.core.router;

import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.parser.ast.DeleteStatement;
import cn.yxffcode.mtd.core.parser.ast.InsertStatement;
import cn.yxffcode.mtd.core.parser.ast.SelectStatement;
import cn.yxffcode.mtd.core.parser.ast.SqlStatement;
import cn.yxffcode.mtd.core.parser.ast.StatementUtils;
import cn.yxffcode.mtd.core.parser.ast.UpdateStatement;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static cn.yxffcode.mtd.utils.CollectionUtils.isEmpty;
import static cn.yxffcode.mtd.utils.CollectionUtils.isNotEmpty;

/**
 * Abstract implementation of Router, it's a convenient super class for specific routers.
 *
 * @author gaohang on 15/12/31.
 */
public abstract class AbstractRouter implements Router {

  private final String column;
  private String tableName;

  public AbstractRouter(String column) {
    this.column = column;
  }

  /**
   * FIXME:路由逻辑比较复杂,可用多态替换
   */
  public Set<String> subTableNames(ParsedSqlContext parsedSqlContext,
                                   final ParameterSupplier parameterSupplier) {

    final Set<String> suffixes = Sets.newHashSet();

    //处理头注释表示的强制指定子表
    // FIXME: 16/1/21 在SQL解析的时候处理注释中的别名,目前注释中不支持别名
    for (Map.Entry<String, String> en : parsedSqlContext.getParsedHeadComment().entrySet()) {
      String columnName = en.getKey();
      if (StringUtils.equalsIgnoreCase(columnName, column)) {
        String columnValueFlag = en.getValue();
        Object parameter = parameterSupplier.getParameter(columnValueFlag);
        if (parameter == null) {
          doWithColumnValue(columnValueFlag, suffixes);
        } else {
          doWithColumnValue(parameter, suffixes);
        }
      }
    }

    SqlStatement sqlStatement = parsedSqlContext.getSqlStatement();
    // FIXME: 16/1/17 目前只支持 = , in 所有条件都需要列名与Router的相等,每个分支中都有列名过虑,是否可以先提前做列名的过虑
    if (sqlStatement instanceof SelectStatement) {
      //需要考虑子查询
      SelectStatement select = (SelectStatement) sqlStatement;

      Expression where = select.getWhere();
      if (where == null) {
        //没有条件
        return allSubNames();
      }
      //对SQL子句做广度优先搜索
      dfs(parameterSupplier, suffixes, where, select.getTables());
    } else if (sqlStatement instanceof UpdateStatement) {
      UpdateStatement update = (UpdateStatement) sqlStatement;
      Expression where = update.getWhere();
      if (where == null) {
        return allSubNames();
      }
      //对SQL子句做广度优先搜索
      dfs(parameterSupplier, suffixes, where, update.getTables());
    } else if (sqlStatement instanceof InsertStatement) {
      InsertStatement insertStatement = (InsertStatement) sqlStatement;
      //检查插入的主表名
      ItemsList itemsList = insertStatement.getItemsList();
      Table table = insertStatement.getTable();
      if (!StringUtils.equalsIgnoreCase(table.getName(), this.tableName)) {
        return suffixes;
      }
      //取column
      List<Column> columnsList = insertStatement.getColumns();
      if (isEmpty(columnsList)) {
        return suffixes;
      }
      if (!(itemsList instanceof ExpressionList)) {
        //insert into table_name select xxx这种语句必须强制指定表
        return Collections.emptySet();
      }

      ExpressionList expressionList = (ExpressionList) itemsList;
      List<Expression> expressions = expressionList.getExpressions();
      if (isEmpty(expressions)) {
        return suffixes;
      }
      List<Table> tables = Arrays.asList(table);
      for (int i = 0, j = columnsList.size(); i < j; i++) {
        //同一张表时,通过Column的值做路由,插入语句中第i个列对应第i个值
        doRoute(parameterSupplier, suffixes, tables, columnsList.get(i), expressions.get(i));
      }
    } else if (sqlStatement instanceof DeleteStatement) {
      DeleteStatement delete = (DeleteStatement) sqlStatement;
      Expression where = delete.getWhere();
      if (where == null) {
        return allSubNames();
      }
      dfs(parameterSupplier, suffixes, where, Arrays.asList(delete.getTable()));
    }
    return suffixes.isEmpty() ? allSubNames() : suffixes;
  }

  /**
   * 对Expression做深度优先搜索
   */
  private void dfs(final ParameterSupplier parameterSupplier, final Set<String> suffixes,
                   Expression where, final List<Table> tables) {
    LinkedList<Expression> expressions = Lists.newLinkedList();
    expressions.add(where);
    //因为jsqlparser的JdbcParameter对象没有给出参数的位置,
    // 根据jsqlparser解析后的语法树,需要使用深度优先搜索来确定参数的位置
    int jdbcParameterIndex = -1;
    while (!expressions.isEmpty()) {
      Expression exp = expressions.removeFirst();
      if (exp instanceof EqualsTo) {
        EqualsTo eq = (EqualsTo) exp;
        Object rightExpression = eq.getRightExpression();
        if (rightExpression instanceof SubSelect) {
          SelectBody selectBody = ((SubSelect) rightExpression).getSelectBody();
          if (selectBody != null) {
            SelectStatement subselectStatement = StatementUtils.parseSelectBody(selectBody);
            suffixes.addAll(subTableNames(new ParsedSqlContext(StringUtils.EMPTY,
                subselectStatement), parameterSupplier));
          }
        } else if (rightExpression instanceof JdbcParameter) {
          rightExpression = ++jdbcParameterIndex;
        }
        doRoute(parameterSupplier, suffixes, tables, eq.getLeftExpression(), rightExpression);
      }
      if (exp instanceof BinaryExpression) {
        BinaryExpression or = (BinaryExpression) exp;
        Expression leftExpression = or.getLeftExpression();
        Expression rightExpression = or.getRightExpression();
        //必须要先加入右子树,再加入左子树,保证左子树先于右子树遍历
        expressions.addFirst(rightExpression);
        expressions.addFirst(leftExpression);
      } else if (exp instanceof InExpression) {
        InExpression in = (InExpression) exp;
        final Expression leftExpression = in.getLeftExpression();
        ItemsList rightItemsList = in.getRightItemsList();
        if (rightItemsList instanceof SubSelect) {
          doRoute(parameterSupplier, suffixes, tables, leftExpression, rightItemsList);
        } else if (rightItemsList instanceof ExpressionList) {
          List<Expression> inElements = ((ExpressionList) rightItemsList).getExpressions();
          if (isNotEmpty(inElements)) {
            for (Expression inElement : inElements) {
              if (inElement instanceof JdbcParameter) {
                ++jdbcParameterIndex;
                doRoute(parameterSupplier, suffixes, tables, leftExpression, jdbcParameterIndex);
              } else {
                doRoute(parameterSupplier, suffixes, tables, leftExpression, inElement);
              }
            }
          }
        } else if (rightItemsList instanceof MultiExpressionList) {
          List<ExpressionList> exprList = ((MultiExpressionList) rightItemsList).getExprList();
          if (isNotEmpty(exprList)) {
            for (ExpressionList expressionList : exprList) {
              List<Expression> inElements = ((ExpressionList) expressionList).getExpressions();
              if (isNotEmpty(inElements)) {
                for (Expression inElement : inElements) {
                  if (inElement instanceof JdbcParameter) {
                    ++jdbcParameterIndex;
                    doRoute(parameterSupplier, suffixes, tables, leftExpression,
                        jdbcParameterIndex);
                  } else {
                    doRoute(parameterSupplier, suffixes, tables, leftExpression, inElement);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * eg:leftElement = rightElement,一般为Column
   * leftElement >= rightElement
   */
  private void doRoute(ParameterSupplier parameterSupplier, Set<String> suffixes,
                       List<Table> tables, Object leftElement, Object rightElement) {
    if (leftElement instanceof Column && !(leftElement instanceof Function)) {
      String col = ((Column) leftElement).getColumnName();
      if (!StringUtils.equalsIgnoreCase(col, column)) {
        return;
      }
      Table columnAlias = ((Column) leftElement).getTable();
      //带别名时,需要别名匹配
      boolean acceptCol = isAcceptCol(tables, columnAlias);
      if (!acceptCol) {
        return;
      }
      if (rightElement instanceof Integer) {
        Integer index = (Integer) rightElement;
        Object parameter = parameterSupplier.getParameter(index);
        doWithColumnValue(parameter, suffixes);
      } else if (rightElement instanceof LongValue) {
        doWithColumnValue(((LongValue) rightElement).getValue(), suffixes);
      } else if (rightElement instanceof DoubleValue) {
        doWithColumnValue(((DoubleValue) rightElement).getValue(), suffixes);
      } else if (rightElement instanceof StringValue) {
        doWithColumnValue(((StringValue) rightElement).getValue(), suffixes);
      } else {
        //比如子查询,大于小于,暂时不支持
        suffixes.addAll(allSubNames());
      }
    }
  }

  private boolean isAcceptCol(List<Table> tables, Table columnAlias) {
    boolean acceptCol = false;
    for (Table table : tables) {
      Alias tableAlias = table.getAlias();
      if (columnAlias != null && tableAlias == null) {
        //列有表的别名,而表没有别名,则表示列不在此表中
        continue;
      }

      //别名,列名都需要相等才能使用此列做路由,别名相等则表示同一个表,如果没有别名,则列名相等即可
      if ((columnAlias != null && StringUtils
          .equalsIgnoreCase(columnAlias.getName(), tableAlias.getName())) || columnAlias == null) {
        acceptCol = true;
        break;
      }
    }
    return acceptCol;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  protected abstract Set<String> allSubNames();

  protected abstract void doWithColumnValue(Object value, Set<String> suffixes);

}
