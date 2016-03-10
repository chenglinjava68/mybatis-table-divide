package cn.yxffcode.mtd.core.parser.ast;

import java.util.Set;

/**
 * @author gaohang on 16/3/9.
 */
public interface SqlStatement {
  Set<String> getTableNames();
}
