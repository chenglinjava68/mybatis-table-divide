package cn.yxffcode.mtd.core.parser;

import cn.yxffcode.mtd.core.parser.ast.SqlStatement;
import cn.yxffcode.mtd.core.parser.ast.StatementUtils;
import com.google.common.base.Throwables;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.StringReader;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * SQL 解析器的实现类，主要是将SQL解析后存放到cache中，
 * 如果cache中有该条SQL,则直接从cache中取，否则进行parse
 *
 * @author gaohang
 */
public class SQLParserImpl implements SQLParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLParserImpl.class);
  private final SqlStatementCache globalCache = SqlStatementCache.instance();
  private JSqlParser parser = new CCJSqlParserManager();
  ;

  private SQLParserImpl() {
  }

  public static SQLParserImpl getInstance() {
    return SQLParserImplHolder.INSTANCE;
  }

  private SqlStatement nestedParseSql(final String sql) {
    if (sql == null) {
      throw new IllegalArgumentException("sql must not be null");
    }
    //为了防止多次重复初始化，所以使用了future task来确保初始化只进行一次
    // SQL 会被 parse 结果会被缓存起来
    //相对于双重校验锁,FutureTask更加并发友好
    FutureTask<SqlStatement> future = globalCache.getFutureTask(sql);
    if (future == null) {
      future = new FutureTask<>(new Callable<SqlStatement>() {
        public SqlStatement call() throws Exception {
          final SqlStatement sqlStatement = doParseSql(sql);
          LOGGER.debug("successfully parse a sql:{}", sql);
          StringBuilder sb = new StringBuilder();
          LOGGER.debug("parsed sql:" + sb.toString());
          return sqlStatement;
        }
      });
      future = globalCache.setFutureTaskIfAbsent(sql, future);
      //FutureTask.run方法使用CAS保证只执行一次.
      future.run();
    }
    try {
      return future.get();
    } catch (Exception e) {
      Throwables.propagate(e);
      //不会返回null,因为Throwables.propagate会抛出RuntimeException
      return null;
    }
  }

  /**
   * Antlr分析sql，返回用java对象表示的SQL。
   */
  private SqlStatement doParseSql(String sql) {
    try (Reader in = new StringReader(sql)) {
      return StatementUtils.parseStatement(parser.parse(in));
    } catch (Exception e) {
      Throwables.propagate(e);
      return null;
    }
  }

  /**
   * 根据SQL获取对应的javaSQL对象
   *
   * @param sql
   * @return java SQL 对象。 如果cache中没有则返回空
   */
  private SqlStatement getStatement(String sql) {
    try {
      FutureTask<SqlStatement> future = globalCache.getFutureTask(sql);
      if (future == null) {
        return null;
      } else {
        return future.get();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public SqlStatement parse(String sql) {
    try {
      return nestedParseSql(sql);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException(e);
    }
  }


  private static final class SQLParserImplHolder {
    private static final SQLParserImpl INSTANCE = new SQLParserImpl();
  }
}
