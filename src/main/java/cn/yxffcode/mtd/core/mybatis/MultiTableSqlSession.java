package cn.yxffcode.mtd.core.mybatis;

import cn.yxffcode.mtd.core.merger.ResultMerger;
import cn.yxffcode.mtd.core.merger.ResultMergerImpl;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.parser.SQLParser;
import cn.yxffcode.mtd.core.parser.SQLParserImpl;
import cn.yxffcode.mtd.core.rewriter.SqlRewriter;
import cn.yxffcode.mtd.core.rewriter.SqlRewriterImpl;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.result.DefaultMapResultHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 支持分表的SqlSession
 *
 * @author gaohang on 16/2/18.
 */
public class MultiTableSqlSession implements SqlSession {

  private static final Splitter COMA = Splitter.on(',').trimResults();
  private static final Splitter COLON = Splitter.on(':').trimResults();

  private SQLParser sqlParser = SQLParserImpl.getInstance();
  private ResultMerger resultMerger = ResultMergerImpl.getInstance();
  private SqlRewriter sqlRewriter = SqlRewriterImpl.getInstance();

  private Configuration configuration;
  private Executor executor;

  private boolean dirty;

  public MultiTableSqlSession(Configuration configuration, Executor executor) {
    this.configuration = configuration;
    this.executor = executor;
    this.dirty = false;
  }

  public <T> T selectOne(String statement) {
    return this.selectOne(statement, null);
  }

  public <T> T selectOne(String statement, Object parameter) {
    // Popular vote was to return null on 0 results and throw exception on too many.
    List<T> list = this.<T>selectList(statement, parameter);
    if (list.size() == 1) {
      return list.get(0);
    } else if (list.size() > 1) {
      throw new TooManyResultsException(
          "Expected one result (or null) to be returned by selectOne(), but found: " + list.size());
    } else {
      return null;
    }
  }

  public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
    return this.selectMap(statement, null, mapKey, RowBounds.DEFAULT);
  }

  public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
    return this.selectMap(statement, parameter, mapKey, RowBounds.DEFAULT);
  }

  public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey,
                                    RowBounds rowBounds) {
    final List<?> list = selectList(statement, parameter, rowBounds);
    final DefaultMapResultHandler<K, V> mapResultHandler = new DefaultMapResultHandler<K, V>(mapKey,
        configuration.getObjectFactory());
    final DefaultResultContext context = new DefaultResultContext();
    for (Object o : list) {
      context.nextResultObject(o);
      mapResultHandler.handleResult(context);
    }
    Map<K, V> selectedMap = mapResultHandler.getMappedResults();
    return selectedMap;
  }

  public <E> List<E> selectList(String statement) {
    return this.<E>selectList(statement, null);
  }

  public <E> List<E> selectList(String statement, Object parameter) {
    return this.selectList(statement, parameter, RowBounds.DEFAULT);
  }

  public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
    try {
      MappedStatement ms = configuration.getMappedStatement(statement);
      Object params = wrapCollection(parameter);
      return doQueryForList(rowBounds, ms, params);
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  private <E> List<E> doQueryForList(RowBounds rowBounds, MappedStatement ms, Object params)
      throws SQLException {
    BoundSql boundSql = ms.getBoundSql(params);
    String sql = boundSql.getSql().trim();
    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);

    MybatisParameterSupplier parameterSupplier = new MybatisParameterSupplier(boundSql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, sqlParser.parse(sql));
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    Iterator<CharSequence> sqls = sqlRewriter.rewrite(parsedSqlContext, parameterSupplier);

    if (sqls == null) {
      return executor.query(ms, params, rowBounds, Executor.NO_RESULT_HANDLER);
    }
    MappedStatement mappedStatement = null;
    List<Object> all = new ArrayList<>(1);
    while (sqls.hasNext()) {
      CharSequence next = sqls.next();
      if (mappedStatement == null) {
        mappedStatement = copyMappedStatement(ms, boundSql);
      }
      setField(boundSql, "sql", next.toString());
      List<E> result =
          executor.query(mappedStatement, params, rowBounds, Executor.NO_RESULT_HANDLER);
      all.add(result);
    }

    return (List<E>) resultMerger
        .merge(all, parsedSqlContext, new MybatisFieldMappingSupplier(mappedStatement),
            parameterSupplier);
  }

  public void select(String statement, Object parameter, ResultHandler handler) {
    select(statement, parameter, RowBounds.DEFAULT, handler);
  }

  public void select(String statement, ResultHandler handler) {
    select(statement, null, RowBounds.DEFAULT, handler);
  }

  public void select(String statement, Object parameter, RowBounds rowBounds,
                     ResultHandler handler) {
    try {
      MappedStatement ms = configuration.getMappedStatement(statement);
      Object params = wrapCollection(parameter);
      doQueryWithoutResult(parameter, rowBounds, handler, ms, params);
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  private void doQueryWithoutResult(Object parameter, RowBounds rowBounds, ResultHandler handler,
                                    MappedStatement ms, Object params)
      throws SQLException {
    BoundSql boundSql = ms.getBoundSql(params);
    String sql = boundSql.getSql().trim();
    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);

    MybatisParameterSupplier parameterSupplier = new MybatisParameterSupplier(boundSql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, sqlParser.parse(sql));
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    Iterator<CharSequence> sqls = sqlRewriter.rewrite(parsedSqlContext, parameterSupplier);

    if (sqls == null) {
      executor.query(ms, wrapCollection(parameter), rowBounds, handler);
    }
    MappedStatement mappedStatement = null;
    while (sqls.hasNext()) {
      CharSequence next = sqls.next();
      if (mappedStatement == null) {
        mappedStatement = copyMappedStatement(ms, boundSql);
      }
      setField(boundSql, "sql", next.toString());
      executor.query(mappedStatement, params, rowBounds, handler);
    }
  }

  public int insert(String statement) {
    return insert(statement, null);
  }

  public int insert(String statement, Object parameter) {
    return update(statement, parameter);
  }

  public int update(String statement) {
    return update(statement, null);
  }

  public int update(String statement, Object parameter) {
    try {
      dirty = true;
      MappedStatement ms = configuration.getMappedStatement(statement);
      return doUpdate(ms, wrapCollection(parameter));
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error updating database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  private int doUpdate(MappedStatement ms, Object parameter) throws SQLException {

    BoundSql boundSql = ms.getBoundSql(parameter);
    String sql = boundSql.getSql();

    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, sqlParser.parse(sql));
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    Iterator<CharSequence> sqls =
        sqlRewriter.rewrite(parsedSqlContext, new MybatisParameterSupplier(boundSql));
    if (sqls == null) {
      //no rewriter
      return executor.update(ms, parameter);
    }

    MappedStatement nms = null;

    int updateCount = -1;
    for (; sqls.hasNext(); ) {
      if (nms == null) {
        //must create a new MappedStatement because of MappedStatement object is a singleton and
        // shared by all invokers.
        nms = copyMappedStatement(ms, boundSql);
      }
      CharSequence s = sqls.next();
      setField(boundSql, "sql", s.toString());
      if (updateCount < 0) {
        updateCount = 0;
      }
      updateCount += executor.update(nms, parameter);
    }
    if (updateCount < 0) {
      return executor.update(ms, parameter);
    }
    return updateCount;
  }

  public int delete(String statement) {
    return update(statement, null);
  }

  public int delete(String statement, Object parameter) {
    return update(statement, parameter);
  }

  public void commit() {
    commit(false);
  }

  public void commit(boolean force) {
    try {
      executor.commit(isCommitOrRollbackRequired(force));
      dirty = false;
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error committing transaction.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  public void rollback() {
    rollback(false);
  }

  public void rollback(boolean force) {
    try {
      executor.rollback(isCommitOrRollbackRequired(force));
      dirty = false;
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error rolling back transaction.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  public List<BatchResult> flushStatements() {
    try {
      return executor.flushStatements();
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error flushing statements.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  public void close() {
    try {
      executor.close(isCommitOrRollbackRequired(false));
      dirty = false;
    } finally {
      ErrorContext.instance().reset();
    }
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public <T> T getMapper(Class<T> type) {
    return configuration.getMapper(type, this);
  }

  public Connection getConnection() {
    try {
      return executor.getTransaction().getConnection();
    } catch (SQLException e) {
      throw ExceptionFactory.wrapException("Error getting a new connection.  Cause: " + e, e);
    }
  }

  public void clearCache() {
    executor.clearLocalCache();
  }

  private boolean isCommitOrRollbackRequired(boolean force) {
    return dirty || force;
  }

  private Object wrapCollection(final Object object) {
    if (object instanceof List) {
      StrictMap<Object> map = new StrictMap<Object>();
      map.put("list", object);
      return map;
    } else if (object != null && object.getClass().isArray()) {
      StrictMap<Object> map = new StrictMap<Object>();
      map.put("array", object);
      return map;
    }
    return object;
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

  private String normalizeSql(String sql) {
    int sqlStart = sql.indexOf("*/");
    if (sqlStart >= 0) {
      sql = sql.substring(sqlStart + "*/".length());
    }
    return sql;
  }

  public static class StrictMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -5741767162221585340L;

    @Override
    public V get(Object key) {
      if (!super.containsKey(key)) {
        throw new BindingException(
            "Parameter '" + key + "' not found. Available parameters are " + this.keySet());
      }
      return super.get(key);
    }

  }

}
