package cn.yxffcode.mtd.core.mybatis.listen;

import cn.yxffcode.mtd.utils.Reflections;
import com.google.common.base.Throwables;
import org.apache.ibatis.binding.MapperMethod;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.reflection.MetaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 支持参数为list的插件实现
 * <p/>
 * 在使用in语句时,往往参数为集合或者数组,需要将mapper文件中写<forEach>,此插件
 * 的目的是为了将forEach从mapper文件中去掉,SQL直接支持in(#{list})
 *
 * @author gaohang on 16/3/3.
 */
public class ListParameterResolver implements MappedStatementListener {

  private static final Logger
      LOGGER = LoggerFactory.getLogger(ListParameterResolver.class);

  @Override public void onMappedStatement(ListenerContext context) {
    if (context.getParameter() == null) {
      return;
    }
    BoundSql boundSql = context.getModifiableMappedStatement().getBoundSql(context.getParameter());
    //绑定参数
    List<ParameterMapping> parameterMappings =
        Collections.unmodifiableList(boundSql.getParameterMappings());
    if (parameterMappings == null || parameterMappings.isEmpty()) {
      return;
    }
    MetaObject mo = MetaObject.forObject(context.getParameter());

    //先记录集合参数的位置
    List<ListParamWrapper> paramPositions =
        buildListParameterPositions(boundSql, parameterMappings, mo);

    if (paramPositions.isEmpty()) {
      return;
    }

    Map<String, Object> paramMap =
        normalizeParameters(context.getParameter(), parameterMappings, mo);

    rebuildBoundSql(boundSql, parameterMappings, paramPositions, paramMap);

    LOGGER.debug("rebuilded sql:{}", boundSql.getSql());

    context.setParameter(paramMap);
  }

  private void rebuildBoundSql(BoundSql boundSql, List<ParameterMapping> parameterMappings,
                               List<ListParamWrapper> paramPositions,
                               Map<String, Object> paramMap) {
    String sql = boundSql.getSql();
    int lastAppended = -1;
    StringBuilder sb = new StringBuilder();

    //copy ParameterMappings, because of the ParameterMapping list is shared by SqlSource, copy a
    // list is commanded.
    List<ParameterMapping> newParameterMappings = new ArrayList<>();

    int parameterAppendIndex = 0;
    for (ListParamWrapper paramPosition : paramPositions) {
      for (int i = parameterAppendIndex; i < paramPosition.mappingPosition; i++) {
        newParameterMappings.add(parameterMappings.get(i));
      }
      //不再需要原始参数,所以第paramPosition.mappingPosition个参数不需要加入新的参数列表
      parameterAppendIndex = paramPosition.mappingPosition + 1;
      int sqlPosition = paramPosition.sqlPosition;
      sb.append(sql.substring(++lastAppended, sqlPosition));
      //拼问号
      int count = 0;
      for (Object obj : paramPosition.params) {
        sb.append('?').append(',');
        String newProperty = paramPosition.paramName + count;
        paramMap.put(newProperty, obj);

        ParameterMapping npm = copyParameterMappingForNewProperty(paramPosition, newProperty);

        newParameterMappings.add(npm);
        count++;
      }
      if (sb.charAt(sb.length() - 1) == ',') {
        sb.deleteCharAt(sb.length() - 1);
      }
      //因为遍历iterable的时候已经append了?,所以原始SQL中的?应该丢弃
      lastAppended = sqlPosition;
    }

    sb.append(sql.substring(lastAppended + 1));

    for (int i = parameterAppendIndex; i < parameterMappings.size(); i++) {
      newParameterMappings.add(parameterMappings.get(i));
    }
    Reflections.setField(boundSql, "parameterMappings", newParameterMappings);
    Reflections.setField(boundSql, "sql", sb.toString());
  }

  private Map<String, Object> normalizeParameters(Object parameter,
                                                  List<ParameterMapping> parameterMappings,
                                                  MetaObject mo) {
    Map<String, Object> paramMap = null;
    //注意参数的处理,参数可能是map,也可能是普通对象,当参数是普通对象时,需要转换成map
    if (parameter != null && parameter instanceof Map) {
      paramMap = (Map<String, Object>) parameter;
    }
    if (paramMap == null) {
      paramMap = new MapperMethod.MapperParamMap<>();
      for (int k = 0, s = parameterMappings.size(); k < s; k++) {
        ParameterMapping pm = parameterMappings.get(k);
        paramMap.put(pm.getProperty(), mo.getValue(pm.getProperty()));
      }
    }
    return paramMap;
  }

  private ParameterMapping copyParameterMappingForNewProperty(
      ListParamWrapper paramPosition, String newProperty) {
    try {
      ParameterMapping pm = paramPosition.parameterMapping;
      MetaObject pmmo = MetaObject.forObject(pm);
      Constructor<ParameterMapping> constructor = ParameterMapping.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      ParameterMapping npm = constructor.newInstance();
      MetaObject npmmo = MetaObject.forObject(npm);
      //copy
      npmmo.setValue("configuration", pmmo.getValue("configuration"));
      npmmo.setValue("property", newProperty);
      npmmo.setValue("mode", pmmo.getValue("mode"));
      npmmo.setValue("javaType", pmmo.getValue("javaType"));
      npmmo.setValue("jdbcType", pmmo.getValue("jdbcType"));
      npmmo.setValue("numericScale", pmmo.getValue("numericScale"));
      npmmo.setValue("typeHandler", pmmo.getValue("typeHandler"));
      npmmo.setValue("resultMapId", pmmo.getValue("resultMapId"));
      npmmo.setValue("jdbcTypeName", pmmo.getValue("jdbcTypeName"));
      return npm;
    } catch (Exception e) {
      Throwables.propagate(e);
      return null;
    }
  }

  private List<ListParamWrapper> buildListParameterPositions(BoundSql boundSql,
                                                             List<ParameterMapping> parameterMappings,
                                                             MetaObject mo) {
    List<ListParamWrapper> positionMap = new ArrayList<>(2);
    for (int i = 0, j = parameterMappings.size(); i < j; i++) {
      ParameterMapping parameterMapping = parameterMappings.get(i);
      final Object value = mo.getValue(parameterMapping.getProperty());
      if (!(value instanceof Iterable) && !value.getClass().isArray()) {
        continue;
      }
      Iterable<?> iterable;
      if (value instanceof Iterable) {
        //找到第i+1个?的位置修改SQL
        iterable = (Iterable<?>) value;
      } else {
        iterable = new AbstractList<Object>() {
          Object[] array = (Object[]) value;

          @Override public Object get(int index) {
            return array[index];
          }

          @Override public int size() {
            return array.length;
          }
        };
      }

      if (iterable == null) {
        throw new NullPointerException(
            "list or array is null,parameter:" + parameterMapping.getProperty());
      }

      String sql = boundSql.getSql();
      int paramIndex = 0;
      for (int k = 0, s = sql.length(); k < s; k++) {
        char c = sql.charAt(k);
        if (c == '?') {
          ++paramIndex;
          if (paramIndex == i + 1) {//第i个参数的位置为k
            positionMap.add(new ListParamWrapper(iterable, k, parameterMapping.getProperty(), i,
                parameterMapping));
          }
        }
      }
    }
    return positionMap;
  }

  private static final class ListParamWrapper {
    private final Iterable<?> params;
    private final int sqlPosition;
    private final String paramName;
    private final int mappingPosition;
    private final ParameterMapping parameterMapping;

    public ListParamWrapper(Iterable<?> params, int sqlPosition, String paramName,
                            int mappingPosition, ParameterMapping parameterMapping) {
      this.params = params;
      this.sqlPosition = sqlPosition;
      this.paramName = paramName;
      this.mappingPosition = mappingPosition;
      this.parameterMapping = parameterMapping;
    }
  }
}
