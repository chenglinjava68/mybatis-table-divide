package cn.yxffcode.mtd.utils;

import cn.yxffcode.mtd.core.mybatis.BoundSqlSource;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;

/**
 * @author gaohang on 16/3/3.
 */
public final class MappedStatementUtils {
  private MappedStatementUtils() {
  }

  public static MappedStatement copyBoundMappedStatement(MappedStatement ms, BoundSql boundSql) {
    return copyMappedStatement(ms, new BoundSqlSource(boundSql));
  }

  public static MappedStatement copyMappedStatement(MappedStatement ms, SqlSource sqlSource) {
    MappedStatement nms = new MappedStatement.Builder(ms.getConfiguration(), ms.getId(), sqlSource,
        ms.getSqlCommandType()).cache(ms.getCache()).databaseId(ms.getDatabaseId())
        .fetchSize(ms.getFetchSize()).flushCacheRequired(true).keyGenerator(ms.getKeyGenerator())
        .parameterMap(ms.getParameterMap()).resource(ms.getResource())
        .resultMaps(ms.getResultMaps()).resultSetType(ms.getResultSetType())
        .statementType(ms.getStatementType()).timeout(ms.getTimeout()).useCache(ms.isUseCache())
        .build();
    Reflections.setField(nms, "keyColumns", ms.getKeyColumns());
    Reflections.setField(nms, "keyProperties", ms.getKeyProperties());
    return nms;
  }

}
