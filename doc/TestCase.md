###测试表结构
```
CREATE TABLE `testFlashback2` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nameShort` varchar(20) DEFAULT NULL,
  `nameLong` varchar(260) DEFAULT NULL,
  `amount` decimal(19,9) DEFAULT NULL,
  `amountFloat` float DEFAULT NULL,
  `amountDouble` double DEFAULT NULL,
  `createDatetime6` datetime(6) DEFAULT NULL,
  `createDatetime` datetime DEFAULT NULL,
  `createTimestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `nameText` text,
  `nameBlob` blob,
  `nameMedium` mediumtext,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
```
####插入回滚
  ```
flush logs
insert into testFlashback2(nameShort,nameLong,amount,amountFloat,amountDouble,createDatetime6,createDatetime,createTimestamp,nameText,nameBlob,nameMedium) values('aaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',10.5,10.6,10.7,'2017-10-26 10:00:00','2017-10-26 10:00:00','2017-10-26 10:00:00','cccc','dddd','eee');
insert into testFlashback2(nameShort,nameLong,amount,amountFloat,amountDouble,createDatetime6,createDatetime,createTimestamp,nameText,nameBlob,nameMedium) values('aaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',10.5,10.6,10.7,'2017-10-26 10:00:00','2017-10-26 10:00:00','2017-10-26 10:00:00','cccc','dddd','eee');
flush logs;
./binary/flashback --binlogFileNames=/var/lib/mysql/haha.000048
    在当前运行目录下产生binlog_output_base.flashback文件
mysqlbinlog --skip-gtids  binlog_output_base.flashback | mysql --socket=/var/lib/mysql/mysql.sock test
    执行结果
mysql> select * from testFlashback2;
Empty set (0.00 sec)
```
    插入回滚成功
####删除回滚
  ```
delete from testFlashback2;
insert into testFlashback2(nameShort,nameLong,amount,amountFloat,amountDouble,createDatetime6,createDatetime,createTimestamp,nameText,nameBlob,nameMedium) values('aaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',10.5,10.6,10.7,'2017-10-26 10:00:00','2017-10-26 10:00:00','2017-10-26 10:00:00','cccc','dddd','eee');
flush logs;
delete from testFlashback2;
./binary/flashback --binlogFileNames=/var/lib/mysql/haha.000050
    在当前运行目录下产生binlog_output_base.flashback文件
mysqlbinlog --skip-gtids  binlog_output_base.flashback | mysql --socket=/var/lib/mysql/mysql.sock test
    执行结果
+----+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+-------------+--------------+----------------------------+---------------------+---------------------+----------+----------+------------+
| id | nameShort | nameLong                                                                                                                                                                                                      | amount       | amountFloat | amountDouble | createDatetime6            | createDatetime      | createTimestamp     | nameText | nameBlob | nameMedium |
+----+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+-------------+--------------+----------------------------+---------------------+---------------------+----------+----------+------------+
|  4 | aaa       | bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb | 10.500000000 |        10.6 |         10.7 | 2017-10-26 10:00:00.000000 | 2017-10-26 10:00:00 | 2017-10-26 10:00:00 | cccc     | dddd     | eee        |
+----+-----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------+-------------+--------------+----------------------------+---------------------+---------------------+----------+----------+------------+
```
####更新回滚
  ```
delete from testFlashback2;
insert into testFlashback2(nameShort,nameLong,amount,amountFloat,amountDouble,createDatetime6,createDatetime,createTimestamp,nameText,nameBlob,nameMedium) values('aaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',10.111,10.6,10.7,'2017-10-26 10:00:00','2017-10-26 10:00:00','2017-10-26 10:00:00','cccc','dddd','eee');
flush logs;
mysql> checksum table testFlashback2;
+---------------------+-----------+
| Table               | Checksum  |
+---------------------+-----------+
| test.testFlashback2 | 717087411 |
+---------------------+-----------+
update testFlashback2 set amount=10.222;
mysql> checksum table testFlashback2;
+---------------------+------------+
| Table               | Checksum   |
+---------------------+------------+
| test.testFlashback2 | 3797190846 |
+---------------------+------------+
./binary/flashback --binlogFileNames=/var/lib/mysql/haha.000052
mysqlbinlog --skip-gtids  binlog_output_base.flashback | mysql --socket=/var/lib/mysql/mysql.sock test
mysql> checksum table testFlashback2;
+---------------------+-----------+
| Table               | Checksum  |
+---------------------+-----------+
| test.testFlashback2 | 717087411 |
+---------------------+-----------+
 ```

####更新回滚（beta环境业务表，总数据量100w，回滚1k数据）
  ```
CREATE TABLE `DP_FeedBackForPOI_For_Flashback` (
`FeedID` int(11) NOT NULL AUTO_INCREMENT,
`UserName` varchar(50) DEFAULT NULL,
`UserEmail` varchar(100) DEFAULT NULL,
`UserPhone` varchar(20) DEFAULT NULL,
`UserID` int(11) DEFAULT '0',
`FeedTitle` varchar(200) DEFAULT NULL,
`FeedComments` text,
`FeedAdddate` datetime DEFAULT NULL,
`FeedStatus` tinyint(3) unsigned DEFAULT '0',
`SendTitle` varchar(200) DEFAULT NULL,
`SendComments` text,
`SendDate` datetime DEFAULT NULL,
`MailTO` varchar(100) DEFAULT NULL,
`FeedType` tinyint(3) unsigned DEFAULT '0',
`LastAdminID` smallint(6) DEFAULT NULL,
`ReferID` int(11) DEFAULT '0',
`ReferUserID` int(11) DEFAULT '0',
`ReferShopID` int(11) DEFAULT '0',
`FeedGroupID` int(11) DEFAULT '0',
`CauseType` tinyint(4) DEFAULT '0',
`ClientType` tinyint(4) DEFAULT '0',
`LastAdminName` varchar(50) DEFAULT NULL,
PRIMARY KEY (`FeedID`),
KEY `LastAdminID` (`LastAdminID`) USING BTREE,
KEY `FeedStatus` (`FeedStatus`,`FeedAdddate`,`FeedType`) USING BTREE,
KEY `MailTO` (`MailTO`,`FeedAdddate`,`FeedStatus`) USING BTREE,
KEY `FeedType` (`FeedType`,`FeedAdddate`,`UserEmail`) USING BTREE,
KEY `FeedGroupID` (`FeedGroupID`) USING BTREE,
KEY `FeedID_FeedStatus` (`FeedStatus`,`FeedGroupID`) USING BTREE,
KEY `UserName` (`UserName`,`MailTO`,`FeedType`,`FeedGroupID`) USING BTREE,
KEY `IX_UserID` (`UserID`) USING BTREE,
KEY `IX_ReferUserID` (`ReferUserID`) USING BTREE,
KEY `IX_ReferShopID` (`ReferShopID`) USING BTREE,
KEY `FeedType_2` (`FeedType`,`ReferID`)
) ENGINE=InnoDB AUTO_INCREMENT=2078564 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT;
mysql> checksum table DP_FeedBackForPOI_For_Flashback;
+---------------------------------+-----------+
| Table                           | Checksum  |
+---------------------------------+-----------+
| DP_FeedBackForPOI_For_Flashback | 717087411 |
+---------------------------------+-----------+
update DP_FeedBackForPOI_For_Flashback set UserName='wangguangyou' limit 1000;
mysql> checksum table DP_FeedBackForPOI_For_Flashback;
+---------------------------------+-----------+
| Table                           | Checksum  |
+---------------------------------+-----------+
| DP_FeedBackForPOI_For_Flashback | 3532811761|
+---------------------------------+-----------+
./binary/flashback --binlogFileNames=/var/lib/mysql/haha.000058
mysqlbinlog --skip-gtids  binlog_output_base.flashback | mysql --socket=/var/lib/mysql/mysql.sock test
mysql> checksum table DP_FeedBackForPOI_For_Flashback;
+---------------------------------+-----------+
| Table                           | Checksum  |
+---------------------------------+-----------+
| DP_FeedBackForPOI_For_Flashback | 717087411 |
+---------------------------------+-----------+
```
