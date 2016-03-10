package cn.yxffcode.mtd.core.mybatis;

import cn.yxffcode.mtd.core.ParameterSupplier;
import org.apache.ibatis.builder.xml.dynamic.ForEachSqlNode;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

import java.util.List;

/**
 * 为SQL的路由提供参数
 *
 * @author gaohang on 16/2/18.
 */
class MybatisParameterSupplier implements ParameterSupplier {

  private final BoundSql boundSql;

  public MybatisParameterSupplier(BoundSql boundSql) {
    this.boundSql = boundSql;
  }

  @Override public int getParameterCount() {
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    return parameterMappings == null ? 0 : parameterMappings.size();
  }

  /**
   * @see org.apache.ibatis.executor.parameter.DefaultParameterHandler
   */
  @Override public Object getParameter(int parameterIndex) {
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    if (parameterMappings != null) {

      ParameterMapping parameterMapping = parameterMappings.get(parameterIndex);

      Object parameterObject = boundSql.getParameterObject();

      if (parameterMapping.getMode() != ParameterMode.OUT) {
        String propertyName = parameterMapping.getProperty();
        return getValue(propertyName, parameterObject);
      }
    }
    return null;
  }

  @Override public Object getParameter(String propertyName) {
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    if (parameterMappings != null) {
      Object parameterObject = boundSql.getParameterObject();
      return getValue(propertyName, parameterObject);
    }
    return null;
  }

  private Object getValue(String propertyName, Object parameterObject) {
    Object value = null;
    PropertyTokenizer prop = new PropertyTokenizer(propertyName);
    if (parameterObject == null) {
      value = null;
    } else if (boundSql.hasAdditionalParameter(propertyName)) {
      value = boundSql.getAdditionalParameter(propertyName);
    } else if (propertyName.startsWith(ForEachSqlNode.ITEM_PREFIX) && boundSql
        .hasAdditionalParameter(prop.getName())) {
      value = boundSql.getAdditionalParameter(prop.getName());
      if (value != null) {
        value =
            MetaObject.forObject(value).getValue(propertyName.substring(prop.getName().length()));
      }
    } else if (parameterObject instanceof Number || parameterObject instanceof String) {
      value = parameterObject;
    } else {
      MetaObject metaObject = MetaObject.forObject(parameterObject);
      if (metaObject.hasGetter(propertyName)) {
        value = metaObject.getValue(propertyName);
      }
    }
    return value;
  }
}
