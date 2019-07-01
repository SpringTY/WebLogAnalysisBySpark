# 课程网站日志分析

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

需求包含的所有信息隐含在第一步输出结果中，但不够明确
因此进行特征提取

根据Ip地址 查出城市
根据url 分类出课程或者文章
根据url 分类出课程号或者文章号

输入 : url + "\t" + dateTime + "\t" + traffic + "\t" + ipAddress
输出 : 
