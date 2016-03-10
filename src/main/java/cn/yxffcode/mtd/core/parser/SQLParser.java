package cn.yxffcode.mtd.core.parser;

import cn.yxffcode.mtd.core.parser.ast.SqlStatement;

/**
 * SQL解析器,用作奖SQL语句解析成抽象语法树,使用Java对象来表示SQL
 *
 * @author gaohang
 */
public interface SQLParser {
  SqlStatement parse(String sql);
}
