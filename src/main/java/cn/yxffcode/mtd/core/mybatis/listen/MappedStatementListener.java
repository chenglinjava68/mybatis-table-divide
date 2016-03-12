package cn.yxffcode.mtd.core.mybatis.listen;

/**
 * 传入的MappedStatement对象可修改,且不会重复绑定sql
 *
 * @author gaohang on 16/3/11.
 */
public interface MappedStatementListener {
  void onMappedStatement(ListenerContext context);
}
