/**
 * 不直接支持复杂SQL,太复杂的情况需要在DAO上处理.
 * <p/>
 * 目前支持基于分表规则字段的查询和更新,对于column > x and column < y这样的查询以及带有函数查询的不支持,可在SQL前
 * 加上注释强行表示需要访问的表.
 *
 * @author gaohang on 16/1/7.
 */
package cn.yxffcode.mtd;
