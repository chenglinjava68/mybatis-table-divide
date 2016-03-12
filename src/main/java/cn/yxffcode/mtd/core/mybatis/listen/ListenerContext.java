package cn.yxffcode.mtd.core.mybatis.listen;

import org.apache.ibatis.mapping.MappedStatement;

/**
 * @author gaohang on 16/3/11.
 */
public class ListenerContext {

  /**
   * 此对象可以被修改
   */
  private MappedStatement modifiableMappedStatement;
  private Object parameter;

  public ListenerContext(MappedStatement modifiableMappedStatement, Object parameter) {
    this.modifiableMappedStatement = modifiableMappedStatement;
    this.parameter = parameter;
  }

  public MappedStatement getModifiableMappedStatement() {
    return modifiableMappedStatement;
  }

  public void setModifiableMappedStatement(MappedStatement modifiableMappedStatement) {
    this.modifiableMappedStatement = modifiableMappedStatement;
  }

  public Object getParameter() {
    return parameter;
  }

  public void setParameter(Object parameter) {
    this.parameter = parameter;
  }
}
