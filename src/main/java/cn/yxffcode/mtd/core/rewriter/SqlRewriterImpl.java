package cn.yxffcode.mtd.core.rewriter;

import cn.yxffcode.mtd.config.Configuration;
import cn.yxffcode.mtd.core.ParameterSupplier;
import cn.yxffcode.mtd.core.parser.ParsedSqlContext;
import cn.yxffcode.mtd.core.router.Router;
import cn.yxffcode.mtd.lang.ImmutableIterator;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 对原始SQL做重写,将原始SQL转换成目标SQL,转换后的目标SQL可能不只一条.
 * <p/>
 * 比如原始SQL查询3张表,分别为a, b, c,这三张表分别需要改写成3个子表,则目标SQL有27条.
 *
 * @author gaohang on 15/12/29.
 */
public class SqlRewriterImpl implements SqlRewriter {

  private static final Set<String> EMPTY_SUB_TABLE_SET = Collections.emptySet();
  private Configuration configuration = Configuration.getInstance();

  private SqlRewriterImpl() {
  }

  public static SqlRewriterImpl getInstance() {
    return SqlRewriterImplHolder.INSTANCE;
  }

  @Override public Iterator<CharSequence> rewrite(final ParsedSqlContext parsedSqlContext,
                                                  ParameterSupplier parameterSupplier) {
    Set<String> tableNames = parsedSqlContext.getSqlStatement().getTableNames();

    List<Map<String/*逻辑表名*/, String/*实际表名*/>> tables = Lists.newLinkedList();

    //依次计算两个集合的笛卡尔积
    for (String tableName : tableNames) {
      Router router = configuration.getRouter(tableName);
      Set<String> subnames = getSubTableNames(parsedSqlContext, parameterSupplier, router);
      if (CollectionUtils.isEmpty(subnames)) {
        continue;
      }
      if (tables.isEmpty()) {
        for (String subname : subnames) {
          Map<String, String> element = Maps.newHashMap();
          element
              .put(tableName, new StringBuilder(tableName).append('_').append(subname).toString());
          tables.add(element);
        }
      } else {
        //笛卡尔集
        Iterator<String> iterator = subnames.iterator();
        //至少有一个元素
        if (iterator.hasNext()) {

          String first = iterator.next();

          //为了减少map中元素的copy次数,先处理后面的元素,最后再处理第一个元素最后处理
          //更新所有已经确定的集合
          int len = tables.size();
          while (iterator.hasNext()) {
            String next = iterator.next();
            //创建新的map
            for (int i = 0; i < len; i++) {
              Map<String, String> element = tables.get(i);
              HashMap<String, String> newElement = Maps.newHashMap(element);
              newElement.put(tableName, next);
            }
          }

          //处理第一个元素
          for (int i = 0; i < len; i++) {
            Map<String, String> element = tables.get(i);
            element
                .put(tableName, new StringBuilder(tableName).append('_').append(first).toString());
          }
        }
      }
    }
    if (CollectionUtils.isEmpty(tables)) {
      //没有子表
      return new ImmutableIterator<CharSequence>() {
        private boolean hasNext = true;

        @Override public boolean hasNext() {
          return hasNext;
        }

        @Override public CharSequence next() {
          hasNext = false;
          return parsedSqlContext.getOriginSql();
        }
      };
    }
    //转换SQL,使用Lazy的方式可在访问DB出错的情况下减少最终SQL的生成
    return Iterators
        .transform(tables.iterator(), new Function<Map<String, String>, CharSequence>() {
          @Override public CharSequence apply(Map<String, String> tab) {
            return replcaeMultiTableName(parsedSqlContext.getOriginSql(), tab);
          }
        });
  }

  private Set<String> getSubTableNames(ParsedSqlContext parsedSqlContext,
                                       ParameterSupplier parameterSupplier, Router router) {
    Set<String> names = router == null ?
        EMPTY_SUB_TABLE_SET :
        router.subTableNames(parsedSqlContext, parameterSupplier);
    if (CollectionUtils.isEmpty(names)) {
      return EMPTY_SUB_TABLE_SET;
    }
    return names;
  }

  /**
   * 替换SQL语句中虚拟表名为实际表名。 会 替换_tableName$ 替换_tableName_ 替换tableName.
   * 替换tableName( 增加替换 _tableName, ,tableName, ,tableName_
   *
   * @param originalSql SQL语句
   * @param virtualName 虚拟表名
   * @param actualName  实际表名
   * @return 返回替换后的SQL语句。
   */
  private String replaceTableName(String originalSql, String virtualName, String actualName) {
    boolean padding = false;
    if (virtualName.equalsIgnoreCase(actualName)) {
      return originalSql;
    }
    List<Object> sqlPieces;
    List<Object> tmpPieces = parseAPatternBegin(virtualName, originalSql,
        new StringBuilder("\\s").append(virtualName).append("$").toString(), padding);

    tmpPieces = parseAPattern(virtualName, tmpPieces,
        new StringBuilder("\\s").append(virtualName).append("\\s").toString(), padding);
    tmpPieces = parseAPattern(virtualName, tmpPieces,
        new StringBuilder(".").append(virtualName).append("\\.").toString(), padding);
    tmpPieces = parseAPattern(virtualName, tmpPieces,
        new StringBuilder("\\s").append(virtualName).append("\\(").toString(), padding);
    tmpPieces = parseAPatternByCalcTable(virtualName, tmpPieces,
        new StringBuilder("//*+.*").append("_").append(virtualName).append("_").append(".*/*/")
            .toString(), padding);
    tmpPieces = parseAPattern(virtualName, tmpPieces,
        new StringBuilder("\\s").append(virtualName).append("\\,").toString(), padding);
    tmpPieces = parseAPattern(virtualName, tmpPieces,
        new StringBuilder("\\,").append(virtualName).append("\\s").toString(), padding);
    // 替换,tableName,
    tmpPieces = parseAPattern(virtualName, tmpPieces,
        new StringBuilder("\\,").append(virtualName).append("\\,").toString(), padding);
    sqlPieces = tmpPieces;
    // 生成最终SQL
    StringBuilder buffer = new StringBuilder();
    boolean first = true;
    for (Object piece : sqlPieces) {
      if (!(piece instanceof String)) {
        throw new IllegalArgumentException("should not be here ! table is " + piece);
      }
      if (!first) {
        buffer.append(actualName);
      } else {
        first = false;
      }
      buffer.append(piece);
    }

    return buffer.toString();
  }

  private String replcaeMultiTableName(String originalSql, Map<String, String> tableToBeReplaced) {
    boolean padding = true;

    if (tableToBeReplaced.size() == 1) {
      for (Map.Entry<String, String> entry : tableToBeReplaced.entrySet()) {
        return replaceTableName(originalSql, entry.getKey(), entry.getValue());
      }
    }
    List<Object> sqlPieces = null;
    for (Map.Entry<String, String> entry : tableToBeReplaced.entrySet()) {
      String virtualName = entry.getKey();
      // tab$
      if (sqlPieces == null) {
        // 第一次进入，第二次以后进入就会有sqlPieces了
        sqlPieces = parseAPatternBegin(virtualName, originalSql,
            new StringBuilder("\\s").append(virtualName).append("$").toString(), padding);
      } else {
        // tab$
        sqlPieces = parseAPattern(virtualName, sqlPieces,
            new StringBuilder("\\s").append(virtualName).append("$").toString(), padding);
      }

      // tab
      sqlPieces = parseAPattern(virtualName, sqlPieces,
          new StringBuilder("\\s").append(virtualName).append("\\s").toString(), padding);
      // table.
      sqlPieces = parseAPattern(virtualName, sqlPieces,
          new StringBuilder(".").append(virtualName).append("\\.").toString(), padding);
      // tab(
      sqlPieces = parseAPattern(virtualName, sqlPieces,
          new StringBuilder("\\s").append(virtualName).append("\\(").toString(), padding);
      // /*+ hint */
      sqlPieces = parseAPatternByCalcTable(virtualName, sqlPieces,
          new StringBuilder("//*+.*").append("_").append(virtualName).append("_").append(".*/*/")
              .toString(), padding);
      sqlPieces = parseAPattern(virtualName, sqlPieces,
          new StringBuilder("\\s").append(virtualName).append("\\,").toString(), padding);
      sqlPieces = parseAPattern(virtualName, sqlPieces,
          new StringBuilder("\\,").append(virtualName).append("\\s").toString(), padding);
      // 替换,tableName,
      sqlPieces = parseAPattern(virtualName, sqlPieces,
          new StringBuilder("\\,").append(virtualName).append("\\,").toString(), padding);
    }

    // 生成最终SQL
    StringBuilder buffer = new StringBuilder();
    for (Object piece : sqlPieces) {
      if (piece instanceof String) {
        buffer.append(piece);
      } else if (piece instanceof LogicTable) {
        buffer.append(tableToBeReplaced.get(((LogicTable) piece).logictable));
      }
    }
    return buffer.toString();
  }

  private List<Object> parseAPatternBegin(String virtualName, String originalSql, String pattern,
                                          boolean padding) {
    Pattern pattern1 = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    List<Object> pieces1 = new LinkedList<Object>();
    Matcher matcher1 = pattern1.matcher(originalSql);
    int start1 = 0;
    while (matcher1.find(start1)) {
      pieces1.add(originalSql.substring(start1, matcher1.start() + 1));
      start1 = matcher1.end();
      if (padding) {
        // TODO: 大小写要验证一下
        pieces1.add(new LogicTable(virtualName));
      }
    }

    pieces1.add(originalSql.substring(start1));
    return pieces1;
  }

  private List<Object> parseAPatternByCalcTable(String virtualName, List<Object> pieces,
                                                String pattern, boolean padding) {
    List<Object> pieces2 = new LinkedList<Object>();
    for (Object piece : pieces) {
      if (piece instanceof String) {
        String strpiece = (String) piece;
        Pattern pattern2 = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher2 = pattern2.matcher(strpiece);
        int start2 = 0;
        while (matcher2.find(start2)) {
          int tableNameStart = matcher2.group().toUpperCase().indexOf(virtualName.toUpperCase())
              //+ start2;
              + matcher2.start();
          int tableNameEnd = tableNameStart + virtualName.length();
          pieces2.add(strpiece.substring(start2, tableNameStart));
          start2 = tableNameEnd;
          if (padding) {
            pieces2.add(new LogicTable(virtualName));
          }
        }
        pieces2.add(strpiece.substring(start2));
      } else {
        pieces2.add(piece);
      }
    }
    return pieces2;
  }

  private List<Object> parseAPattern(String virtualName, List<Object> pieces, String pattern,
                                     boolean padding) {
    List<Object> result = new LinkedList<Object>();
    for (Object piece : pieces) {
      if (piece instanceof String) {
        String strpiece = (String) piece;
        Pattern pattern2 = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher2 = pattern2.matcher(strpiece);
        int fromIndex = 0;
        while (matcher2.find(fromIndex)) {
          result
              .add(strpiece.substring(fromIndex - 1 < 0 ? 0 : fromIndex - 1, matcher2.start() + 1));
          fromIndex = matcher2.end();
          if (padding) {
            result.add(new LogicTable(virtualName));
          }
        }
        result.add(strpiece.substring(fromIndex - 1 < 0 ? 0 : fromIndex - 1));
      } else {
        result.add(piece);
      }
    }
    return result;
  }

  private static final class SqlRewriterImplHolder {
    private static final SqlRewriterImpl INSTANCE = new SqlRewriterImpl();
  }


  private static final class LogicTable {
    public String logictable;

    public LogicTable(String logicTable) {
      this.logictable = logicTable;
    }

    public String toString() {
      return "logictable:" + logictable;
    }
  }
}
