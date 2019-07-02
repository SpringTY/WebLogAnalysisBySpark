# 课程网站日志分析

## 踩坑点(开发中持续更新)

###在需求二分析时的超级大坑(已解决)
https://github.com/SpringTY/WebLogAnalysisBySpark/issues/1

## 规划

### 需求分析

+ 根据次数统计网站最受欢迎的Top N的课程/笔记
+ 根据地区统计网站最受欢迎的Top N的课程
+ 根据流量统计网站最受欢迎的Top N的课程

### 数据源解析

数据源结构示例

> 222.129.51.111 - - [10/Nov/2016:00:01:01 +0800] "POST /course/ajaxmediauser/ HTTP/1.1" 200 54 "www.imooc.com" "http://www.imooc.com/video/4155" mid=4155&time=59.998999999999796&learn_time=336.8 "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36" "-" 10.100.136.64:80 200 0.017 0.017
> 120.52.92.4 - - [10/Nov/2016:00:01:01 +0800] "POST /api3/getcourseintro HTTP/1.1" 200 3618 "www.imooc.com" "-" cid=743&timestamp=1478707259407&uid=3195819&secrect=c1c7420571804f29f262099a975b0e11&token=5638e3e382ae9bf83bd7f467e9d1646b "mukewang/5.0.1 (Android 6.0; LeEco Le X621 Build/HCXCNCT5801708221S),Network WIFI" "-" 10.100.136.64:80 200 1.550 1.550
> 140.207.223.186 - - [10/Nov/2016:00:01:01 +0800] "POST /api3/getcourseintro HTTP/1.1" 200 2322 "www.imooc.com" "-" uid=2651249&cid=137&token=595caac2abacdee25fd16ca70aea08f5 "mukewang/4.3.0 (Android 4.4.4; samsung SM-G5306W Build/KTU84P),Network WIFI" "-" 10.100.136.65:80 200 0.012 0.012

## 清洗

### 第一次清洗(过滤)

**实现类：com.spring.FirstFormat**

根据需求对原始日志进行采集有用信息

1.ip地址 用于统计地区访问需求
2.url 用于确定访问课程或者笔记等
3.流量 用于统计流量访问需求
4.时间 用于次数访问需求

输入格式 : 日志数据
输出格式 : url + "\t" + dateTime + "\t" + traffic + "\t" + ipAddress

坑 : SimpleDateFormat线程不安全，采用FastDateFormat

经过第一次数据清洗后 数据格式如下：

```log
http://www.imooc.com/code/138	2016-11-10 00:20:57	54	119.139.196.179
http://www.imooc.com/code/1358	2016-11-10 00:20:57	67	221.130.126.109
-	2016-11-10 00:20:57	103	117.136.40.194
http://www.imooc.com/video/12597	2016-11-10 00:20:57	6046	119.137.73.138
http://www.imooc.com/code/6075	2016-11-10 00:20:57	54	140.120.13.186
http://www.imooc.com/video/1459	2016-11-10 00:20:57	54	113.73.102.29
http://www.imooc.com/video/8525	2016-11-10 00:20:57	54	180.117.32.221
http://www.imooc.com/video/4037	2016-11-10 00:20:57	6319	121.69.33.13
http://www.imooc.com/video/263	2016-11-10 00:20:57	54	101.227.12.253
http://www.imooc.com/video/9910	2016-11-10 00:20:57	54	59.40.75.66
-	2016-11-10 00:20:57	1590	182.98.132.101
http://www.imooc.com/video/5867	2016-11-10 00:20:57	141	117.57.207.57
```

### 第二次清洗(提取特征)

**实现类：com.spring.jobs.FirstFormat**
需求包含的所有信息隐含在第一步输出结果中，但不够明确
因此进行特征提取

根据Ip地址 查出城市
根据url 分类出课程或者文章
根据url 分类出课程号或者文章号

按照day分组输出

输入 : url + "\t" + dateTime + "\t" + traffic + "\t" + ipAddress
输出 : parquet

输出部分结果

```file
part-00101-af31c9fe-ce4d-4b59-9300-3b65f0bb0b6c.snappy.parquet
```

借助ipdatabase来确定Ip地区

**注 : 导入github开源的Maven项目到工程**

1) clone 下来

```bash
git clone github地址
```

2) clean并且打包

```bash
mvn clean package -DskipTests
```

3） 添加到本地Maven库

```bash
mvn install:install-file -Dfile=/Users/spring/Downloads/temp/ipdatabase/target/ipdatabase-1.0-SNAPSHOT.jar -DgroupId=com.ggstar -DartifactId=ipdatabase -Dversion=1.0 -Dpackaging=jar
```

4）导入原项目的Resource资源和pom.xml中依赖

## 统计分析阶段

### 根据访问次数选择出受欢迎的课程

在spark中，用第二次清洗好的数据用DataFrame创建一个View来用sparksql分析
然后存入MYSQL数据库

创建数据库的SQL:

```sql
create table videoTimes(
    day varchar(8) not null,
    cmsid bigint(10) not null,
    times bigint(10) not null,
    primary key (day,cmsid)
)
create table articleTimes(
    day varchar(8) not null,
    cmsid bigint(10) not null,
    times bigint(10) not null,
    primary key (day,cmsid)
)
```

然后通过JDBC编程写入数据库
相关类:com.spring.jobs.AnalysisTopN
看一下最后数据库中表

```sql
mysql> select * from videoTimes order by times desc limit 10;
+----------+-------+-------+
| day      | cmsid | times |
+----------+-------+-------+
| 20161110 |   180 | 26065 |
| 20161110 |  1230 | 20119 |
| 20161110 |   141 | 19438 |
| 20161110 |   145 | 19274 |
| 20161110 |   669 | 18658 |
| 20161110 |   366 | 18380 |
| 20161110 |   981 | 18099 |
| 20161110 |   324 | 17840 |
| 20161110 |   133 | 16616 |
| 20161110 |   162 | 16332 |
+----------+-------+-------+
10 rows in set (0.01 sec)

mysql> select count(*) from videoTimes;
+----------+
| count(*) |
+----------+
|     4160 |
+----------+
1 row in set (0.01 sec)
```

### 根据地区选择出受欢迎的课程

需求：找出每个地区中访问次数top3

**操作类:com.spring.jobs.AnalysisTopN**

```sql
create table videoRegion(
    day varchar(8) not null,
    city varchar(10) not null,
    cmsid bigint(10) not null,
    times bigint(10) not null,
    times_rank bigint(10) not null,
    primary key (day,city,cmsid)
)
```

输入:第二次format后log信息
输出:mysql数据库

输出Sample:

```sql
mysql> select * from videoRegion order by city,times_rank limit 10;
+----------+-----------+-------+-------+------------+
| day      | city      | cmsid | times | times_rank |
+----------+-----------+-------+-------+------------+
| 20161110 | 上海市    |   323 |  2082 |          1 |
| 20161110 | 上海市    |   324 |  2046 |          2 |
| 20161110 | 上海市    |   180 |  1837 |          3 |
| 20161110 | 中国      |   237 |   117 |          1 |
| 20161110 | 中国      |  2397 |    46 |          2 |
| 20161110 | 中国      |   239 |    22 |          3 |
| 20161110 | 云南省    |   245 |  4282 |          1 |
| 20161110 | 云南省    |  1246 |   993 |          2 |
| 20161110 | 云南省    |   333 |   527 |          3 |
| 20161110 | 全球      |   144 |  2079 |          1 |
+----------+-----------+-------+-------+------------+
```

学习点:

1)控制输出文件大小 coalesce

2)分区字段数据类型调整，禁止dataframe自动推测

3)多次添加数据段用批处理，关闭自动提交

### 根据流量选择出受欢迎的课程

**相关类:com.spring.jobs.AnalysisTopN**
在mysql中建表

```sql
create table videoTraffic(
    day varchar(8) not null,
    cmsid bigint(10) not null,
    traffics bigint(10) not null,
    primary key (day,cmsid)
)
```

输入:第二次format后log信息
输出:mysql数据库

输出Sample:
```sql
mysql> select * from videoTraffic order by traffics desc limit 10 ;
+----------+-------+-----------+
| day      | cmsid | traffics  |
+----------+-------+-----------+
| 20161110 |  1332 | 288715956 |
| 20161110 |   447 | 284947425 |
| 20161110 |  1112 | 240083210 |
| 20161110 |   150 | 224446391 |
| 20161110 |  1206 | 199841153 |
| 20161110 |  1310 | 198135084 |
| 20161110 |   577 | 193204232 |
| 20161110 |   958 | 192223091 |
| 20161110 |   590 | 190557642 |
| 20161110 |   338 | 189025822 |
+----------+-------+-----------+
10 rows in set (0.00 sec)
```
