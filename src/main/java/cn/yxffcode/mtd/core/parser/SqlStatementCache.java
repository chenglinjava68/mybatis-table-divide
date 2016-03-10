package cn.yxffcode.mtd.core.parser;

import cn.yxffcode.mtd.core.parser.ast.SqlStatement;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import net.sf.jsqlparser.statement.Statement;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 可有于缓存解析后的SQL,实际上缓存的是{@link Statement}对象
 *
 * @author gaohang
 */
public class SqlStatementCache {

  private static final int DEFAULT_CACHE_SIZE = 389;
  private final int capacity;
  private final Map<String, StatementItem> cache;
  private final ReentrantLock lock = new ReentrantLock();
  private SqlStatementCache() {
    int size = DEFAULT_CACHE_SIZE;
    String propSize = System.getProperty("limiku.db.parser.cachesize");
    if (propSize != null) {
      size = NumberUtils.toInt(propSize, DEFAULT_CACHE_SIZE);
    }
    capacity = size;
    ConcurrentLinkedHashMap<String, StatementItem> map =
        new ConcurrentLinkedHashMap.Builder<String, StatementItem>()
            .initialCapacity(capacity)
            .maximumWeightedCapacity(capacity).build();
    this.cache = map;
  }

  public static final SqlStatementCache instance() {
    return ParserCacheHolder.INSTANCE;
  }

  public int size() {
    return cache.size();
  }

  private StatementItem get(String sql) {
    return cache.get(sql);
  }

  public FutureTask<SqlStatement> getFutureTask(String sql) {
    StatementItem StatementItem = get(sql);
    if (StatementItem != null) {
      return StatementItem.getFutureStatement();
    } else {
      return null;
    }

  }

  public FutureTask<SqlStatement> setFutureTaskIfAbsent(String sql,
                                                        FutureTask<SqlStatement> future) {
    StatementItem StatementItem = get(sql);
    FutureTask<SqlStatement> returnFutureTask;
    if (StatementItem == null) {
      //完全没有的情况，在这种情况下，肯定是因为还没有现成请求过解析某条sql
      lock.lock();
      try {
        // 双检查lock
        StatementItem = get(sql);
        if (StatementItem == null) {

          StatementItem = new StatementItem();

          put(sql, StatementItem);
        }
      } finally {

        lock.unlock();
      }
      //cas 更新ItemValue中的Statement对象
      returnFutureTask = StatementItem.setFutureStatementIfAbsent(future);

    } else if (StatementItem.getFutureStatement() == null) {
      //cas 更新ItemValue中的Statement对象
      returnFutureTask = StatementItem.setFutureStatementIfAbsent(future);
    } else {
      returnFutureTask = StatementItem.getFutureStatement();
    }

    return returnFutureTask;

  }

  protected void put(String sql, StatementItem StatementItem) {
    cache.put(sql, StatementItem);
  }


  private static final class ParserCacheHolder {
    private static final SqlStatementCache INSTANCE = new SqlStatementCache();
  }


  private static final class StatementItem {

    /**
     * 缓存的整个sql
     */
    private AtomicReference<FutureTask<SqlStatement>> futureStatement =
        new AtomicReference<>();

    public FutureTask<SqlStatement> getFutureStatement() {
      return futureStatement.get();
    }

    public FutureTask<SqlStatement> setFutureStatementIfAbsent(FutureTask<SqlStatement> future) {
      //如果原值为null则会原子的设置新值进去。并且返回新值
      if (futureStatement.compareAndSet(null, future)) {
        return future;
      } else {
        //如果里面的值已经不为null，则读取该值
        return futureStatement.get();
      }
    }

  }
}
