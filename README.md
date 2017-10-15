### 一、简介
MyFlash是由美团点评公司技术工程部开发维护的一个回滚DML操作的工具。该工具通过解析v4版本的binlog，完成回滚操作。相对已有的回滚工具，其增加了更多的过滤选项，让回滚更加容易。
**该工具已经在美团点评内部使用**
### 二、详细说明
1. [安装](./doc/INSTALL.md)
2. [使用](./doc/how_to_use.md)
3. [测试用例](./doc/TestCase.md)
### 三、限制
1. binlog格式必须为row,且binlog_row_image=full
2. 仅支持5.6与5.7
3. 只能回滚DML（增、删、改）
### 四、FAQ
1. 实现的原理是什么？   
- 答：参考文章http://url.cn/5yVTfLY

2. 支持gtid吗？  
- 答：支持。请参考 [使用](./doc/how_to_use.md)

3. 在开启gtid的MySQL server上，应用flashback报错，错误为：ERROR 1782 (HY000) at line 16: @@SESSION.GTID_NEXT cannot be set to ANONYMOUS when @@GLOBAL.GTID_MODE = ON.  ?     
- 答：在导入时加入--skip-gtids
mysqlbinlog --skip-gtids <flashbacklog> | mysql -uxxx -pxxx

4. 如果回滚后的binlog日志尺寸超过20M，在导入时，很耗时。如何处理?      
- 答：参考 [使用](./doc/how_to_use.md) ,搜索maxSplitSize。使用该参数可以对文件进行切片
### 五、联系方式
QQ群:645702809
