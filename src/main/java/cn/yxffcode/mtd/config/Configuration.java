package cn.yxffcode.mtd.config;

import cn.yxffcode.mtd.core.router.Router;
import cn.yxffcode.mtd.core.router.Routers;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.core.io.ClassPathResource;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

/**
 * The configuration for multimybatis.
 *
 * @author gaohang on 15/12/29.
 */
public class Configuration {

  public static final String CONFIG_NAME = "multimybatis.xml";

  /**
   * keys are table name and values are router class.
   */
  private Map<String, Router> routers;

  private Configuration() {
  }

  public static Configuration getInstance() {
    return ConfigurationHolder.INSTANCE;
  }

  private void config()
      throws ParserConfigurationException, IOException, SAXException, XPathExpressionException,
      ClassNotFoundException {
    DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    ClassPathResource resource = new ClassPathResource(CONFIG_NAME);
    try (InputStream in = resource.getInputStream()) {
      Document document = documentBuilder.parse(in);
      XPathFactory xpathFactory = XPathFactory.newInstance();
      XPath xpath = xpathFactory.newXPath();
      NodeList nodeList = (NodeList) xpath.evaluate("/dal/route", document, XPathConstants.NODESET);
      if (nodeList == null || nodeList.getLength() == 0) {
        routers = Collections.emptyMap();
      }
      parseRouters(nodeList);
    }

    configRouters();
  }

  private void parseRouters(NodeList nodeList) throws ClassNotFoundException {
    if (nodeList == null || nodeList.getLength() == 0) {
      routers = Collections.emptyMap();
    } else {
      if (routers == null) {
        routers = Maps.newHashMapWithExpectedSize(nodeList.getLength());
      }
      for (int i = 0, j = nodeList.getLength(); i < j; i++) {
        Node routeNode = nodeList.item(i);
        String tableName = routeNode.getAttributes().getNamedItem("table").getTextContent();
        String strategy = routeNode.getAttributes().getNamedItem("strategy").getTextContent();
        int funcStart = strategy.indexOf('(');
        if (funcStart < 0) {
          //class
          Router router = (Router) BeanUtils.instantiate(Class.forName(strategy));
          routers.put(tableName, router);
          continue;
        }
        int funcEnd = strategy.indexOf(')');
        if (funcEnd < 0) {
          throw new ConfigurationException(
              "strategy error, must be a class name or the exists strategy:" + routeNode);
        }
        String strategyFlag = strategy.substring(0, funcStart).trim();
        for (Routers r : Routers.values()) {
          if (StringUtils.equals(r.func(), strategyFlag)) {
            Router router = r.generateRouter(strategy.substring(funcStart + 1, funcEnd));
            routers.put(tableName, router);
          }
        }
      }
    }
    if (routers == null) {
      routers = Collections.emptyMap();
    } else {
      routers = Collections.unmodifiableMap(routers);
    }
  }

  private void configRouters() {
    for (Map.Entry<String, Router> en : routers.entrySet()) {
      en.getValue().setTableName(en.getKey());
    }
  }

  public Router getRouter(String tableName) {
    return routers.get(tableName);
  }

  private static final class ConfigurationHolder {
    private static final Configuration INSTANCE = new Configuration();

    static {
      try {
        INSTANCE.config();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}
