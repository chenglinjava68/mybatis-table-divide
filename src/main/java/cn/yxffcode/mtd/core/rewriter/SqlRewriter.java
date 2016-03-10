package cn.yxffcode.mtd.core.rewriter;

import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;

import java.util.Iterator;

/**
 * Rewrite the source sql to target sql list.
 *
 * @author gaohang on 15/12/29.
 */
public interface SqlRewriter {
  Iterator<CharSequence> rewrite(ParsedSqlContext parsedSqlContext,
                                 ParameterSupplier parameterSupplier);
}
