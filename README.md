# mybatis-table-divide
基于mybatis的分表工具

利用mybatis实现分表功能，可定制分表规则  
##问题  
基于SqlSession实现，但实现方式不太好，fork了mybatis中的DefaultSqlSession的代码，使用代理的方式更好  
查询结果合并的代码需要重构，太复杂了  
  
my blog:http://www.yxffcode.cn/index.php/archives/29

