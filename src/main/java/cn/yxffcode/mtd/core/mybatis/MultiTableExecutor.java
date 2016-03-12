package cn.yxffcode.mtd.core.mybatis;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.limiku.spider.multimybatis.core.ast.SqlStatement;
import com.limiku.spider.multimybatis.core.merger.ResultMerger;
import com.limiku.spider.multimybatis.core.merger.ResultMergerImpl;
import com.limiku.spider.multimybatis.core.parser.ParsedSqlContext;
import com.limiku.spider.multimybatis.core.parser.SQLParser;
import com.limiku.spider.multimybatis.core.parser.SQLParserImpl;
import com.limiku.spider.multimybatis.core.rewriter.SqlRewriter;
import com.limiku.spider.multimybatis.core.rewriter.SqlRewriterImpl;
import com.limiku.spider.multimybatis.utils.MappedStatementUtils;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 支持分表
 * <p/>
 * 因为需要修改MappedStatement中的Sql,所以原始的MappedStatement需要被复制才能执行,不能修改原始SQL
 * <p/>
 * 当且仅当传入此对象的MappedStatement是通过{@link BoundSqlSource}复杂过的才可以不需要再次复制,此时
 * {@link #mappedStatementCopyOnWrite}可以为false.
 *
 * @author gaohang on 16/3/11.
 */
public class MultiTableExecutor implements Executor {

  private static final Splitter COMA = Splitter.on(',').trimResults();
  private static final Splitter COLON = Splitter.on(':').trimResults();

  private SQLParser sqlParser = SQLParserImpl.getInstance();
  private ResultMerger resultMerger = ResultMergerImpl.getInstance();
  private SqlRewriter sqlRewriter = SqlRewriterImpl.getInstance();

  private final Executor internalExecutor;
  private final boolean mappedStatementCopyOnWrite;

  public MultiTableExecutor(Executor internalExecutor) {
    this(internalExecutor, true);
  }

  public MultiTableExecutor(Executor internalExecutor, boolean mappedStatementCopyOnWrite) {
    this.internalExecutor = internalExecutor;
    this.mappedStatementCopyOnWrite = mappedStatementCopyOnWrite;
  }

  @Override public int update(MappedStatement ms, Object parameter) throws SQLException {
    if (mappedStatementCopyOnWrite) {
      return doUpdate(MappedStatementUtils.copyBoundMappedStatement(ms,
          ms.getBoundSql(parameter)), parameter);
    } else {
      return doUpdate(ms, parameter);
    }
  }

  @Override public <E> List<E> query(MappedStatement ms,
                                     Object parameter, RowBounds rowBounds,
                                     ResultHandler resultHandler,
                                     CacheKey cacheKey,
                                     BoundSql boundSql) throws SQLException {
    if (mappedStatementCopyOnWrite) {
      return doQueryForList(MappedStatementUtils.copyBoundMappedStatement(ms,
          ms.getBoundSql(parameter)), parameter, rowBounds, resultHandler, cacheKey, boundSql);
    } else {
      return doQueryForList(ms, parameter, rowBounds, resultHandler, cacheKey, boundSql);
    }
  }

  @Override public <E> List<E> query(MappedStatement ms,
                                     Object parameter, RowBounds rowBounds,
                                     ResultHandler resultHandler) throws SQLException {
    if (mappedStatementCopyOnWrite) {
      return doQueryForList(MappedStatementUtils.copyBoundMappedStatement(ms,
          ms.getBoundSql(parameter)), parameter, rowBounds, resultHandler, null, null);
    } else {
      return doQueryForList(ms, parameter, rowBounds, resultHandler, null, null);
    }
  }

  private int doUpdate(MappedStatement ms, Object parameter) throws SQLException {

    BoundSql boundSql = ms.getBoundSql(parameter);
    String sql = boundSql.getSql();

    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);
    SqlStatement statement = (SqlStatement) sqlParser.parse(sql, true);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, statement);
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    Iterator<CharSequence> sqls =
        sqlRewriter.rewrite(parsedSqlContext, new MybatisParameterSupplier(boundSql));
    if (sqls == null) {
      //no rewriter
      return internalExecutor.update(ms, parameter);
    }

    int updateCount = -1;
    for (; sqls.hasNext(); ) {
      CharSequence s = sqls.next();
      setField(boundSql, "sql", s.toString());
      if (updateCount < 0) {
        updateCount = 0;
      }
      updateCount += internalExecutor.update(ms, parameter);
    }
    if (updateCount < 0) {
      return internalExecutor.update(ms, parameter);
    }
    return updateCount;
  }

  private <E> List<E> doQueryForList(MappedStatement ms, Object params, RowBounds rowBounds,
                                     ResultHandler resultHandler, CacheKey cacheKey,
                                     BoundSql srcboundSql)
      throws SQLException {
    BoundSql boundSql = srcboundSql == null ? ms.getBoundSql(params) : srcboundSql;
    String sql = boundSql.getSql().trim();
    Map<String, String> parsedHeadComment = parseHeadComment(sql);
    sql = normalizeSql(sql);

    SqlStatement sqlStatement = (SqlStatement) sqlParser.parse(sql, true);

    MybatisParameterSupplier parameterSupplier = new MybatisParameterSupplier(boundSql);
    ParsedSqlContext parsedSqlContext = new ParsedSqlContext(sql, sqlStatement);
    parsedSqlContext.setParsedHeadComment(parsedHeadComment);

    Iterator<CharSequence> sqls = sqlRewriter.rewrite(parsedSqlContext, parameterSupplier);

    if (sqls == null) {
      if (cacheKey == null) {
        cacheKey = createCacheKey(ms, params, rowBounds, boundSql);
      }
      return internalExecutor.query(ms, params, rowBounds, resultHandler, cacheKey, boundSql);
    }
    List<Object> all = new ArrayList<>(1);
    while (sqls.hasNext()) {
      CharSequence next = sqls.next();
      setField(boundSql, "sql", next.toString());
      if (cacheKey == null) {
        cacheKey = createCacheKey(ms, params, rowBounds, boundSql);
      }
      List<E> result =
          internalExecutor.query(ms, params, rowBounds, resultHandler, cacheKey, boundSql);
      all.add(result);
    }

    return (List<E>) resultMerger
        .merge(all, parsedSqlContext, new MybatisFieldMappingSupplier(ms), parameterSupplier);
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

  @Override public List<BatchResult> flushStatements() throws SQLException {
    return internalExecutor.flushStatements();
  }

  @Override public void commit(boolean required) throws SQLException {
    internalExecutor.commit(required);
  }

  @Override public void rollback(boolean required) throws SQLException {
    internalExecutor.rollback(required);
  }

  @Override public CacheKey createCacheKey(MappedStatement ms,
                                           Object parameterObject,
                                           RowBounds rowBounds,
                                           BoundSql boundSql) {
    return internalExecutor.createCacheKey(ms, parameterObject, rowBounds, boundSql);
  }

  @Override public boolean isCached(MappedStatement ms,
                                    CacheKey key) {
    return internalExecutor.isCached(ms, key);
  }

  @Override public void clearLocalCache() {
    internalExecutor.clearLocalCache();
  }

  @Override public void deferLoad(MappedStatement ms,
                                  MetaObject resultObject,
                                  String property, CacheKey key) {
    internalExecutor.deferLoad(ms, resultObject, property, key);
  }

  @Override public Transaction getTransaction() {
    return internalExecutor.getTransaction();
  }

  @Override public void close(boolean forceRollback) {
    internalExecutor.close(forceRollback);
  }

  @Override public boolean isClosed() {
    return internalExecutor.isClosed();
  }
}
