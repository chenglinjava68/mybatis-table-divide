package cn.yxffcode.mtd.core.parser.ast;

/**
 * 简单的函数查询中,函数的类型
 *
 * @author gaohang on 16/3/10.
 */
public enum GroupFunctionType {
  AVG, MAX, MIN, COUNT, SUM,

  /**
   * 表示非简单函数查询
   */
  NONE
}
