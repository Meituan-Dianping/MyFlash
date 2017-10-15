### 1.How to use

```
cd binary
./flashback --help

Usage:
  flashback [OPTION...]

Help Options:
  -?, --help                  Show help options

Application Options:
  --databaseNames             databaseName to apply. if multiple, seperate by comma(,)
  --tableNames                tableName to apply. if multiple, seperate by comma(,)
  --start-position            start position
  --stop-position             stop position
  --start-datetime            start time (format %Y-%m-%d %H:%M:%S)
  --stop-datetime             stop time (format %Y-%m-%d %H:%M:%S)
  --sqlTypes                  sql type to filter . support INSERT, UPDATE ,DELETE. if multiple, seperate by comma(,)
  --maxSplitSize              max file size after split, the uint is M
  --binlogFileNames           binlog files to process. if multiple, seperate by comma(,)
  --outBinlogFileNameBase     output binlog file name base
  --logLevel                  log level, available option is debug,warning,error
  --include-gtids             gtids to process
  --exclude-gtids             gtids to skip
```

### 2.Parameter explantion
下面的这些参数是可以任意组合的。

- 1.databaseNames

  指定需要回滚的数据库名。多个数据库可以用“,”隔开。如果不指定该参数，相当于指定了所有数据库。
- 2.tableNames  

  指定需要回滚的表名。多个表可以用“,”隔开。如果不指定该参数，相当于指定了所有表。
- 3.start-position

  指定回滚开始的位置。如不指定，从文件的开始处回滚。请指定正确的有效的位置，否则无法回滚
- 4.stop-position

  指定回滚结束的位置。如不指定，回滚到文件结尾。请指定正确的有效的位置，否则无法回滚
- 5.start-datetime

  指定回滚的开始时间。注意格式必须是 %Y-%m-%d %H:%M:%S。 如不指定，则不限定时间
- 6.stop-datetime

  指定回滚的结束时间。注意格式必须是 %Y-%m-%d %H:%M:%S。 如不指定，则不限定时间  
- 7.sqlTypes

  指定需要回滚的sql类型。目前支持的过滤类型是INSERT, UPDATE ,DELETE。多个类型可以用“,”隔开。
- 8.maxSplitSize

  *一旦指定该参数，对文件进行固定尺寸的分割（单位为M），过滤条件有效，但不进行回滚操作。该参数主要用来将大的binlog文件切割，防止单次应用的binlog尺寸过大，对线上造成压力*
- 9.binlogFileNames

  指定需要回滚的binlog文件，目前只支持单个文件，后续会增加多个文件支持  
- 10.outBinlogFileNameBase

  指定输出的binlog文件前缀，如不指定，则默认为binlog_output_base.flashback
- 11.logLevel

  仅供开发者使用，默认级别为error级别。在生产环境中不要修改这个级别，否则输出过多
- 12.include-gtids

  指定需要回滚的gtid,支持gtid的单个和范围两种形式。
- 13.exclude-gtids

  指定不需要回滚的gtid，用法同include-gtids


### 3.example

1.回滚整个文件
```
./flashback --binlogFileNames=haha.000041
mysqlbinlog binlog_output_base.flashback | mysql -h<host> -u<user> -p
```
2.回滚该文件中的所有insert语句
```
./flashback  --sqlTypes='INSERT' --binlogFileNames=haha.000041
mysqlbinlog binlog_output_base.flashback | mysql -h<host> -u<user> -p
```
3.回滚大文件
```
回滚
./flashback --binlogFileNames=haha.000042
切割大文件
./flashback --maxSplitSize=1 --binlogFileNames=binlog_output_base.flashback
应用
mysqlbinlog binlog_output_base.flashback.000001 | mysql -h<host> -u<user> -p
...
mysqlbinlog binlog_output_base.flashback.<N> | mysql -h<host> -u<user> -p
```
