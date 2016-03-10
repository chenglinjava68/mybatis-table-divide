# mybatis-table-divide
基于mybatis的分表工具

利用mybatis实现分表功能，可定制分表规则  
##问题  
基于SqlSession实现，但实现方式不太好，fork了mybatis中的DefaultSqlSession的代码，使用代理的方式更好  
部分代码需要重构  
