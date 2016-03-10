package cn.yxffcode.mtd.core.parser;

import cn.yxffcode.mtd.core.parser.ast.SqlStatement;

import java.util.Collections;
import java.util.Map;

/**
 * 表示解析后的SQL,其中包含由SQL解析器{@link SQLParser}解析后的对象以及SQL头部注释
 *
 * @author gaohang on 16/1/17.
 */
public class ParsedSqlContext {
  private final String originSql;
  private final SqlStatement sqlStatement;
  private Map<String, String> parsedHeadComment = Collections.emptyMap();

  public ParsedSqlContext(String originSql, SqlStatement sqlStatement) {
    this.originSql = originSql;
    this.sqlStatement = sqlStatement;
  }

  public String getOriginSql() {
    return originSql;
  }

  public SqlStatement getSqlStatement() {
    return sqlStatement;
  }

  public Map<String, String> getParsedHeadComment() {
    return parsedHeadComment;
  }

  public void setParsedHeadComment(Map<String, String> parsedHeadComment) {
    this.parsedHeadComment = parsedHeadComment;
  }
}
