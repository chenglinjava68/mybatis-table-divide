package cn.yxffcode.mtd.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于标识mybatis映射接口，Spring可通过此注解识别DAO类,给DAO对象注入正确的
 * {@link org.apache.ibatis.session.SqlSession}
 *
 * @author gaohang on 15/7/27.
 * @see com.limiku.spider.multimybatis.core.mybatis.MapperScannerConfigurer
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DDB {

  /**
   * @return bean在Spring IoC中的名字
   */
  String value() default "";

  String dbname();
}
