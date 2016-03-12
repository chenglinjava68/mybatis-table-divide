package cn.yxffcode.mtd.core.mybatis;

import cn.yxffcode.mtd.core.FieldMapping;
import cn.yxffcode.mtd.core.merger.ResultMerger;
import cn.yxffcode.mtd.core.merger.ResultMergerImpl;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.parser.SQLParser;
import cn.yxffcode.mtd.core.parser.SQLParserImpl;
import cn.yxffcode.mtd.core.parser.ast.SqlStatement;
import cn.yxffcode.mtd.core.rewriter.SqlRewriter;
import cn.yxffcode.mtd.core.rewriter.SqlRewriterImpl;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 以mybatis插件的形式处理分表查询与查询结果的合并,缺点是很难并发查询多个子表.
 * <p/>
 * deprecated by {@link MagnificentSqlSession}, {@link MagnificentSqlSessionFactory},
 * 利用插件实现分表与通过SqlSession实现分表,总体流程上一样,实现SqlSession接口复杂一点,但更利于扩展
 * 到分库同时分表的场景.
 *
 * @author gaohang on 15/12/29.
 */
@Intercepts({@Signature(type = Executor.class, method = "query",
    args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
    @Signature(type = Executor.class, method = "query",
        args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class,
            CacheKey.class, BoundSql.class}), @Signature(type = Executor.class, method = "update",
    args = {MappedStatement.class, Object.class})})
public class MatrixTableInterceptor implements Interceptor {

  private static final Splitter COMA = Splitter.on(',').trimResults();
  private static final Splitter COLON = Splitter.on(':').trimResults();

  private static final Class<?>[] FULL_PARAM_CLASSES =
      {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class,
          BoundSql.class};

  private static final int FULL_PARAM_COUNT = FULL_PARAM_CLASSES.length;

  private SQLParser sqlParser = SQLParserImpl.getInstance();
  private SqlRewriter sqlRewriter = SqlRewriterImpl.getInstance();
  private ResultMerger resultMerger = ResultMergerImpl.getInstance();

  @Override public Object intercept(Invocation invocation) throws Throwable {

    switch (invocation.getMethod().getName()) {
      case "query":
        return doQuery(invocation);
      case "update":
        return doUpdate(invocation);
      default:
        throw new UnsupportedOperationException(
            invocation.getMethod().getName() + " is not allowed");
    }
  }

  private Object doUpdate(Invocation invocation) throws Throwable {
    Object[] args = invocation.getArgs();
    MappedStatement ms = (MappedStatement) args[0];
    Object parameter = args[1];

    BoundSql boundSql = ms.getBoundSql(parameter);
    String sql = boundSql.getSql();

    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);
    SqlStatement statement = sqlParser.parse(sql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, statement);
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    Iterator<CharSequence> sqls =
        sqlRewriter.rewrite(parsedSqlContext, new MybatisParameterSupplier(boundSql));
    if (sqls == null) {
      //no rewriter
      return invocation.proceed();
    }

    MappedStatement nms = null;

    List<Object> results = Lists.newArrayList();
    for (; sqls.hasNext(); ) {
      if (nms == null) {
        //must create a new MappedStatement because of MappedStatement object is a singleton and
        // shared by all invokers.
        nms = copyMappedStatement(ms, boundSql);
        args[0] = nms;
      }
      CharSequence s = sqls.next();
      setField(boundSql, "sql", s.toString());
      Object result = invocation.getMethod().invoke(invocation.getTarget(), args);
      results.add(result);
    }
    if (results.size() == 0) {
      return invocation.proceed();
    }
    if (results.size() == 1) {
      return results.get(0);
    }
    return resultMerger.merge(results, parsedSqlContext, null, null);
  }

  private MappedStatement copyMappedStatement(MappedStatement ms, BoundSql boundSql) {
    return copyMappedStatement(ms, new BoundSqlSource(boundSql));
  }

  private MappedStatement copyMappedStatement(MappedStatement ms, SqlSource sqlSource) {
    MappedStatement nms;
    nms = new MappedStatement.Builder(ms.getConfiguration(), ms.getId(), sqlSource,
        ms.getSqlCommandType()).cache(ms.getCache()).databaseId(ms.getDatabaseId())
        .fetchSize(ms.getFetchSize()).flushCacheRequired(true).keyGenerator(ms.getKeyGenerator())
        .parameterMap(ms.getParameterMap()).resource(ms.getResource())
        .resultMaps(ms.getResultMaps()).resultSetType(ms.getResultSetType())
        .statementType(ms.getStatementType()).timeout(ms.getTimeout()).useCache(ms.isUseCache())
        .build();
    setField(nms, "keyColumns", ms.getKeyColumns());
    setField(nms, "keyProperties", ms.getKeyProperties());
    return nms;
  }

  private void setField(Object target, String field, Object value) {
    Field ss = ReflectionUtils.findField(target.getClass(), field);
    ss.setAccessible(true);
    ReflectionUtils.setField(ss, target, value);
  }

  private Object doQuery(Invocation invocation)
      throws InvocationTargetException, IllegalAccessException, SQLException {

    Object[] args = invocation.getArgs();
    final MappedStatement ms = (MappedStatement) args[0];

    final BoundSql boundSql = args.length == FULL_PARAM_COUNT ?
        (BoundSql) args[FULL_PARAM_COUNT - 1] :
        ms.getBoundSql(args[1]);
    String sql = boundSql.getSql().trim();

    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);
    SqlStatement statement = sqlParser.parse(sql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, statement);
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    MybatisParameterSupplier parameterSupplier = new MybatisParameterSupplier(boundSql);
    Iterator<CharSequence> sqls = sqlRewriter.rewrite(parsedSqlContext, parameterSupplier);
    if (sqls == null) {
      //no rewriter
      return invocation.proceed();
    }

    MappedStatement mappedStatement = null;
    List<Object> results = new ArrayList<>();
    for (; sqls.hasNext(); ) {
      CharSequence next = sqls.next();
      if (next == null || next.length() == 0) {
        continue;
      }
      setField(boundSql, "sql", next.toString());
      //需要使用修改后的MappedStatement,不能使用原始的,需要保证原始对象不变
      if (mappedStatement == null) {
        mappedStatement = copyMappedStatement(ms, boundSql);
        args[0] = mappedStatement;
      }
      Object result = invocation.proceed();
      results.add(result);
    }
    return resultMerger.merge(results, parsedSqlContext, new Supplier<FieldMapping>() {
      @Override public FieldMapping get() {
        return new FieldMapping() {
          @Override public String map(String col) {
            /*
             * do not need to O(n) performance because of order by statement usually contains just a
             * few columns and transform a list to a map is also overhead.
             */
            List<ResultMap> resultMaps = ms.getResultMaps();
            for (int i = 0, j = resultMaps.size(); i < j; i++) {
              ResultMap resultMap = resultMaps.get(i);
              List<ResultMapping> mappings = resultMap.getResultMappings();
              if (CollectionUtils.isEmpty(mappings)) {
                continue;
              }
              for (int k = 0, l = mappings.size(); k < l; k++) {
                ResultMapping mapping = mappings.get(k);
                if (StringUtils.equals(mapping.getColumn(), col)) {
                  return mapping.getProperty();
                }
              }
            }
            return col;
          }
        };
      }
    }, parameterSupplier);
  }

  private String normalizeSql(String sql) {
    int sqlStart = sql.indexOf("*/");
    if (sqlStart >= 0) {
      sql = sql.substring(sqlStart + "*/".length());
    }
    return sql;
  }

  private Map<String, String> parseHeadComment(String sql) {
    //解析SQL的头部注释
    if (sql.startsWith("/*")) {
      int end = sql.indexOf("*/");
      String comment = sql.substring("/*".length(), end);
      Iterable<String> cols = COMA.split(comment);
      Map<String, String> parsedHeadComment = Maps.newHashMap();
      for (String col : cols) {
        List<String> commentElems = COLON.splitToList(col);
        if (commentElems.size() == 2) {
          parsedHeadComment.put(commentElems.get(0), commentElems.get(1));
        }
      }
      return parsedHeadComment;
    }
    return Collections.emptyMap();
  }

  @Override public Object plugin(Object target) {
    if (target instanceof Executor) {
      return Plugin.wrap(target, this);
    } else {
      return target;
    }
  }

  @Override public void setProperties(Properties properties) {
  }

}
