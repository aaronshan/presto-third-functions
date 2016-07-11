# presto-third-functions
[Readme in English](https://github.com/aaronshan/presto-third-functions/tree/master/README-en.md)

## 简介

包含了一些presto自定义的函数

## 构建
```
cd ${project_home}
mvn clean package
```

执行完命令后,将会生成在target目录下presto-third-functions-1.0-SNAPSHOT-shaded.jar`文件.

## 函数
| 函数| 说明|
|:--|:--|
|dayofweek(date_string \| date) -> int |计算给定日期是每周7天内的第几天,其中周一返回1,周天返回7,错误返回-1.|
|pinyin(string) -> int | 将汉字转为拼音|
|zodiac(date_string \| date) -> string | 将日期转换为星座英文 |
|zodiac_cn(date_string \| date) -> string | 将日期转换为星座中文 | 
|id_card_province(string) -> string |由身份证号获取省份|
|id_card_city(string) -> string |由身份证号获取城市|
|id_card_area(string) -> string |由身份证号获取区或县|
|id_card_birthday(string) -> string |由身份证号获取出生日期|
|id_card_gender(string) -> string |由身份证号获取性别|
|is_valid_id_card(string) -> boolean |鉴别是否是有效的身份证号|
|wgs_distance(double lat1, double lng1, double lat2, double lng2) -> double |计算WGS84坐标系下的坐标距离|
|gcj_to_bd(double,double) -> json |火星坐标系(GCJ-02)转百度坐标系(BD-09),谷歌、高德——>百度|
|bd_to_gcj(double,double) -> json |百度坐标系(BD-09)转火星坐标系(GCJ-02),百度——>谷歌、高德|
|wgs_to_gcj(double,double) -> json |WGS84转GCJ02(火星坐标系)|
|gcj_to_wgs(double,double) -> json |GCJ02(火星坐标系)转GPS84|

> 关于互联网地图坐标系的说明见: [当前互联网地图的坐标系现状](https://github.com/aaronshan/presto-third-functions/tree/master/src/main/java/cc/shanruifeng/functions/udfs/scalar/geographic/README-geo.md)


## 用法

把presto-third-functions-1.0-SNAPSHOT-shaded.jar放到 `${presto_home}/plugin/hive-hadoop2` 目录下并重启presto.下面是示例:
### 1. 重启presto
```
mv presto-third-functions-1.0-SNAPSHOT-shaded.jar /home/presto/presto-server-0.147/plugin/hive-hadoop2/
cd /home/presto/presto-server-0.147
bin/launcher restart
```

### 2. 设置presto命令
```
cd /home/presto/
ln -s presto-client-0.147/presto-cli-0.147-executable.jar presto-cli
export JAVA_HOME=/home/java8/jdk1.8.0_91/;
export PATH=/home/java8/jdk1.8.0_91/bin/:$PATH;
alias presto="/home/presto/presto-cli --server localhost:8080 --catalog hive --schema default"
```

### 3. 示例
```
presto
presto:default> select dayofweek(my_day) from (values '2016-07-07') as t(my_day);
 _col0
-------
     4
(1 row)

Query 20160707_073523_00005_iya2r, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

presto:default> select pinyin(country) from (values '中国') as t(country);
  _col0
----------
 zhongguo
(1 row)

Query 20160707_073649_00006_iya2r, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

presto:default> select gcj_to_bd(lat,lng), bd_to_gcj(lat,lng), wgs_to_gcj(lat,lng), gcj_to_wgs(lat,lng) from (values (39.915, 116.404)) as t(lat, lng)\G
-[ RECORD 1 ]---------------------------------------------
_col0 | {"lng":116.41036949371029,"lat":39.92133699351022}
_col1 | {"lng":116.39762729119315,"lat":39.90865673957631}
_col2 | {"lng":116.41024449916938,"lat":39.91640428150164}
_col3 | {"lng":116.39775550083061,"lat":39.91359571849836}

Query 20160711_131937_00001_n4waa, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

```