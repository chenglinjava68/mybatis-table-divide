package cn.yxffcode.mtd.core.merger;

import cn.yxffcode.mtd.core.FieldMapping;
import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.parser.ast.GroupFunctionType;
import cn.yxffcode.mtd.core.parser.ast.SelectStatement;
import com.google.common.base.Supplier;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.List;

import static cn.yxffcode.mtd.utils.CollectionUtils.isNotEmpty;

/**
 * 合并查询结果
 *
 * @author gaohang on 15/12/29.
 */
public class ResultMergerImpl implements ResultMerger {

  private ResultMergerImpl() {
  }

  public static ResultMergerImpl getInstance() {
    return ResultMergerImplHolder.INSTANCE;
  }

  @Override public Object merge(final List<Object> results, final ParsedSqlContext parsedSqlContext,
                                final Supplier<FieldMapping> fieldMappingSupplier,
                                final ParameterSupplier parameterSupplier) throws SQLException {
    if (CollectionUtils.isEmpty(results)) {
      return null;
    }

    if (results.size() == 1) {
      return results.get(0);//不需要合并结果
    }

    SelectStatement selectStatement = (SelectStatement) parsedSqlContext.getSqlStatement();
    GroupFunctionType groupFuncType =
        selectStatement.getGroupFuncType();
    if (groupFuncType == null) {
      groupFuncType = GroupFunctionType.NONE;
    }
    switch (groupFuncType) {
      case AVG: {
        return Mergers.AVG
            .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
      }
      case COUNT: {
        return Mergers.COUNT
            .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
      }
      case MAX: {
        return Mergers.MAX
            .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
      }
      case MIN: {
        return Mergers.MIN
            .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
      }
      case SUM: {
        return Mergers.SUM
            .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
      }
      default: {
        if (selectStatement.getDistinct() != null) {
          return Mergers.DISTINCT
              .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
        } else if (isNotEmpty(selectStatement.getOrderByElements())) {
          //order by语句的处理有限制,必须需要查询的列中包含order by的列才能在应用层做合并,否则合并时找不到原始数据,合并失败.
          return Mergers.ORDER_BY
              .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
        } else {
          return Mergers.DEFAULT
              .merge(results, parsedSqlContext, fieldMappingSupplier, parameterSupplier);
        }
      }
    }
  }

  private static final class ResultMergerImplHolder {
    private static final ResultMergerImpl INSTANCE = new ResultMergerImpl();
  }

}
