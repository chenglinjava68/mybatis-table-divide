package cn.yxffcode.mtd.core.merger;

import cn.yxffcode.mtd.core.FieldMapping;
import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import com.google.common.base.Supplier;

import java.sql.SQLException;
import java.util.List;

/**
 * When query multiple table, result may not just one,
 *
 * @author gaohang on 15/12/29.
 */
public interface ResultMerger {

  /**
   * merge result, note that the results parameter may be a list of list.
   */
  Object merge(List<Object> results, ParsedSqlContext parsedSqlContext,
               Supplier<FieldMapping> fieldMappingSupplier, ParameterSupplier parameterSupplier)
      throws SQLException;

}
