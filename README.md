# presto-third-functions [![Build Status](https://travis-ci.org/aaronshan/presto-third-functions.svg?branch=master)](https://travis-ci.org/aaronshan/presto-third-functions)
[Readme in English](https://github.com/aaronshan/presto-third-functions/tree/master/README-en.md)

## 简介

包含了一些presto自定义的函数

## 构建
### 各软件版本:
* Java 8 Update 60 及以上
* Maven 3.3.9+

### 命令:
```
cd ${project_home}
mvn clean package
```

如果想要忽略单元测试,请执行:
```
mvn clean package -DskipTests
```
执行完命令后,将会生成在target目录下presto-third-functions-0.1.0-shaded.jar`文件.

## 函数
### 1. 字符串相关函数
| 函数| 说明|
|:--|:--|
|pinyin(string) -> string | 将汉字转为拼音|
|md5(string) -> string |对字符串求md5值|
|sha256(string) -> string |对字符串求sha256值|

### 2. 日期相关函数
| 函数| 说明|
|:--|:--|
|dayofweek(date_string \| date) -> int |计算给定日期是每周7天内的第几天,其中周一返回1,周天返回7,错误返回-1.|
|zodiac(date_string \| date) -> string | 将日期转换为星座英文 |
|zodiac_cn(date_string \| date) -> string | 将日期转换为星座中文 | 
|typeofdate(date_string \| date) -> string | 获取日期的类型(1: 法定节假日, 2: 正常周末, 3: 正常工作日 4:攒假的工作日),错误返回-1. | 

### 3. 数组相关函数
| 函数| 说明|
|:--|:--|
|array_union(array, array) -> array |求两个array的并集|
|value_count(array(T), T value) -> int | 统计在数组中值为给定值的元素个数|

> 我已经发起了一个`array_union`的[PR](https://github.com/prestodb/presto/pull/5644#event-729329053), 现在它已经被合并到presto的master分支中. 因此,如果你的presto版本 > 0.151,它已经包含了`array_union`函数.

### 4. JSON相关函数
| 函数| 说明|
|:--|:--|
|json_array_extract(json, jsonPath) -> array(varchar) |提取json数组中对应路径的值|
|json_array_extract_scalar(json, jsonPath) -> array(varchar) |和`json_array_extract`类似,但是返回结果是string(不是json格式)|

### 5. MAP相关函数
| 函数| 说明|
|:--|:--|
|value_count(MAP(K,V), V value) -> int | 统计中MAP中值为给定值的元素的个数|

### 6. 身份证相关函数
| 函数| 说明|
|:--|:--|
|id_card_province(string) -> string |由身份证号获取省份|
|id_card_city(string) -> string |由身份证号获取城市|
|id_card_area(string) -> string |由身份证号获取区或县|
|id_card_birthday(string) -> string |由身份证号获取出生日期|
|id_card_gender(string) -> string |由身份证号获取性别|
|is_valid_id_card(string) -> boolean |鉴别是否是有效的身份证号|
|id_card_info(string) -> json |获取身份证号对应的信息,包括省份,城市,区县,性别及是否有效|

### 7. 坐标相关函数
| 函数| 说明|
|:--|:--|
|wgs_distance(double lat1, double lng1, double lat2, double lng2) -> double |计算WGS84坐标系下的坐标距离,单位为米|
|gcj_to_bd(double,double) -> json |火星坐标系(GCJ-02)转百度坐标系(BD-09),谷歌、高德——>百度|
|bd_to_gcj(double,double) -> json |百度坐标系(BD-09)转火星坐标系(GCJ-02),百度——>谷歌、高德|
|wgs_to_gcj(double,double) -> json |WGS84转GCJ02(火星坐标系)|
|gcj_to_wgs(double,double) -> json |GCJ02(火星坐标系)转GPS84,输出的WGS-84坐标精度为1米到2米之间。|
|gcj_extract_wgs(double,double) -> json |GCJ02(火星坐标系)转GPS84,输出的WGS-84坐标精度为0.5米内。但是计算速度慢于gcj_to_wgs|

> 关于互联网地图坐标系的说明见: [当前互联网地图的坐标系现状](https://github.com/aaronshan/presto-third-functions/tree/master/src/main/java/cc/shanruifeng/functions/udfs/scalar/geographic/README-geo.md)

### 8. 其他函数
| 函数| 说明|
|:--|:--|
|is_null(all_type) -> boolean |是否是null|

## 用法

把presto-third-functions-0.1.0-shaded.jar放到 `${presto_home}/plugin/hive-hadoop2` 目录下并重启presto.下面是示例:
### 1. 重启presto
```
mv presto-third-functions-0.1.0-shaded.jar /home/presto/presto-server-0.147/plugin/hive-hadoop2/
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
#### 3.1 字符串相关函数
```
presto:default> select pinyin(country) from (values '中国') as t(country);
  _col0
----------
 zhongguo
(1 row)

Query 20160707_073649_00006_iya2r, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```


```
presto:default> select md5(col1), sha256(col1) from (values 'aaronshan') as t(col1)\G;
-[ RECORD 1 ]-----------------------------------------------------------
_col0 | 95686bc0483262afe170b550dd4544d1
_col1 | d16bb375433ad383169f911afdf45e209eabfcf047ba1faebdd8f6a0b39e0a32

Query 20160712_071936_00006_hkbes, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

#### 3.2 日期相关函数
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
```

#### 3.3 数组相关函数
```
presto:default> select array_union(arr1, arr2) from (values (ARRAY [1,3,5,null], ARRAY [2,3,4,null])) as t(arr1, arr2);
         _col0
-----------------------
 [1, 3, 5, null, 2, 4]
(1 row)

Query 20160713_061707_00004_82kmt, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

```
presto:default> select value_count(arr1, 'a') from (values (ARRAY['a', 'b', 'a'])) t(arr1);
 _col0
-------
     2
(1 row)

Query 20160721_111719_00008_xgf26, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

#### 3.4 JSON相关函数
```
presto:default> select json_array_extract(arr1, '$.book.id') from (values ('[{"book":{"id":"12"}}, {"book":{"id":"14"}}]')) t(arr1);
    _col0
--------------
 ["12", "14"]
(1 row)

Query 20160721_105423_00006_xgf26, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```


```
presto:default> select json_array_extract_scalar(arr1, '$.book.id') from (values ('[{"book":{"id":"12"}}, {"book":{"id":"14"}}]')) t(arr1);
  _col0
----------
 [12, 14]
(1 row)

Query 20160721_105426_00007_xgf26, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

#### 3.6 MAP相关函数
```
presto:default> select map1, value_count(map1, 'a') from (values (map(ARRAY[1,2,3], ARRAY['a', 'b', 'a']))) t(map1);
      map1       | _col1
-----------------+-------
 {1=a, 2=b, 3=a} |     2
(1 row)

Query 20160721_111906_00011_xgf26, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

#### 3.5 身份证相关函数
```
presto:default> select id_card_info(card) from (values '110101198901084517') as t(card);
                                      _col0
----------------------------------------------------------------------------------
 {"area":"东城区","valid":true,"province":"北京市","gender":"男","city":"北京市"}
(1 row)

Query 20160712_071700_00004_hkbes, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

#### 3.6 坐标相关函数
```
presto:default> select gcj_to_bd(lat,lng), bd_to_gcj(lat,lng), wgs_to_gcj(lat,lng), gcj_to_wgs(lat,lng), gcj_extract_wgs(lat,lng) from (values (39.915, 116.404)) as t(lat, lng)\G;
-[ RECORD 1 ]----------------------------------------------
_col0 | {"lng":116.41036949371029,"lat":39.92133699351022}
_col1 | {"lng":116.39762729119315,"lat":39.90865673957631}
_col2 | {"lng":116.41024449916938,"lat":39.91640428150164}
_col3 | {"lng":116.39775550083061,"lat":39.91359571849836}
_col4 | {"lng":116.39775549316407,"lat":39.913596801757805}

Query 20160712_024714_00003_9rund, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```

#### 3.7 其他函数
```
presto:default> select is_null(col0),is_null(col1),is_null(col2),is_null(col3) from (values ('test', 1, 0.5, ARRAY [1]),(null, null, null, null)) as t(col0, col1, col2,col3);
 _col0 | _col1 | _col2 | _col3
-------+-------+-------+-------
 false | false | false | false
 true  | true  | true  | true
(2 rows)

Query 20160713_061435_00003_82kmt, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```