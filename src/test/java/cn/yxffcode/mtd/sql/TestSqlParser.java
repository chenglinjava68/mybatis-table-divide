package cn.yxffcode.mtd.sql;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.Statement;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * @author gaohang on 16/3/3.
 */
public class TestSqlParser {

  @Test
  public void testSqlParser() throws IOException, JSQLParserException {
    JSqlParser parser = new CCJSqlParserManager();

    try (Reader in = new StringReader("select a.id, a.name from table_a a limit 10, 20")) {
      Statement statement = parser.parse(in);
      System.out.println(statement);
    }
  }
}
