package cn.yxffcode.mtd.core.mybatis;

import com.google.common.base.Supplier;
import com.limiku.spider.multimybatis.core.FieldMapping;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author gaohang on 16/2/29.
 */
public class MybatisFieldMappingSupplier implements Supplier<FieldMapping> {
  private final MappedStatement mappedStatement;

  public MybatisFieldMappingSupplier(MappedStatement mappedStatement) {
    this.mappedStatement = mappedStatement;
  }

  @Override public FieldMapping get() {
    return new FieldMapping() {
      @Override public String map(String col) {
            /*
             * do not need to O(n) performance because of order by statement usually contains just a
             * few columns and transform a list to a map is also overhead.
             */
        List<ResultMap> resultMaps = mappedStatement.getResultMaps();
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
}
