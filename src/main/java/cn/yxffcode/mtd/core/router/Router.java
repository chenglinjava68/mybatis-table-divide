package cn.yxffcode.mtd.core.router;

import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;

import java.util.Set;

/**
 * Route a sql to special tables
 *
 * @author gaohang on 15/12/29.
 */
public interface Router {

  Set<String> subTableNames(ParsedSqlContext parsedSqlContext, ParameterSupplier parameterSupplier);

  void setTableName(String tableName);
}
