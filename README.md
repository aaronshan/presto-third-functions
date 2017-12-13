# presto-third-functions 

[![Build Status](https://travis-ci.org/aaronshan/presto-third-functions.svg?branch=master)](https://travis-ci.org/aaronshan/presto-third-functions)
[![Documentation Status](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat)](https://github.com/aaronshan/presto-third-functions/tree/master/README.md)
[![Documentation Status](https://img.shields.io/badge/中文文档-最新-brightgreen.svg)](https://github.com/aaronshan/presto-third-functions/tree/master/README-zh.md)
[![Release](https://img.shields.io/github/release/aaronshan/presto-third-functions.svg)](https://github.com/aaronshan/presto-third-functions/releases)


## Introduction

some custom presto functions

## Build

### requires
* Java 8 Update 60 or higher (8u60+)
* Maven 3.3.9+ (for building)

```
cd ${project_home}
mvn clean package
```

If you want skip unit tests, please run:
```
mvn clean package -DskipTests
```

It will generate presto-third-functions-{version}-shaded.jar in target directory.

You can also directly download file from [release page](https://github.com/aaronshan/presto-third-functions/releases).

### version description
| version | description |
|:--|:--|
| `0.2.0` | support `presto-0.147`~`presto-0.149`|
| `0.3.0` | support `presto-0.150`~`presto-0.151`|
| `0.4.0` | support `presto-0.152`|
| `0.5.0` | support `presto-0.153`~`presto-0.166`|
| `0.5.1` | support `presto-0.167`~`presto-0.168`|

## functions

### 1. string functions
| function| description |
|:--|:--|
|pinyin(string) -> string | convert chinese to pinyin|
|md5(string) -> string | md5 hash|
|sha256(string) -> string |sha256 hash|

### 2. array functions
| function| description |
|:--|:--|
|array_union(array, array) -> array |return union result of two array.|
|value_count(array(T), T value) -> int | count ARRAY's element number that element value equals given value.|

> I had already proposed a [pull request](https://github.com/prestodb/presto/pull/5644#event-729329053) about `array_union`. Currently, it be merged to presto master branch. So, If your presto version > 0.151, it already include array_union function.

To support `presto-0.150+`, from `0.3.0`, it had rename to `arr_union`. (from `0.5.0`, I had delete `arr_union` function, please use `array_union`.)

### 3. date functions
| function| description |
|:--|:--|
|dayofweek(date_string \| date) -> int | day of week,if monday,return 1, sunday return 7, error return -1.|
|zodiac(date_string \| date) -> string | convert date to zodiac|
|zodiac_cn(date_string \| date) -> string | convert date to zodiac chinese | 

### 4. JSON functions
| function| description |
|:--|:--|
|json_array_extract(json, jsonPath) -> array(varchar) |extract json array by given jsonPath.|
|json_array_extract_scalar(json, jsonPath) -> array(varchar) |like `json_array_extract`, but returns the result value as a string (as opposed to being encoded as JSON).|

### 5. MAP functions
| function| description |
|:--|:--|
|value_count(MAP(K,V), V value) -> int | count MAP's element number that element value equals given value.|

### 6. geographic functions 
| function| description |
|:--|:--|
|wgs_distance(double lat1, double lng1, double lat2, double lng2) -> double | calculate WGS84 coordinate distance, in meters. |
|gcj_to_bd(double,double) -> json | GCJ-02(火星坐标系) convert to BD-09(百度坐标系), 谷歌、高德——>百度|
|bd_to_gcj(double,double) -> json | BD-09(百度坐标系) convert to GCJ-02(火星坐标系), 百度——>谷歌、高德|
|wgs_to_gcj(double,double) -> json | WGS84(地球坐标系) convert to GCJ02(火星坐标系)|
|gcj_to_wgs(double,double) -> json | GCJ02(火星坐标系) convert to GPS84(地球坐标系), output coordinate WGS-84 accuracy within 1 to 2 meters.|
|gcj_extract_wgs(double,double) -> json | GCJ02(火星坐标系) convert to GPS84, output coordinate WGS-84 accuracy within 0.5 meters. but compute cost more time than `gcj_to_wgs`. |

> 关于互联网地图坐标系的说明见: [当前互联网地图的坐标系现状](https://github.com/aaronshan/presto-third-functions/tree/master/src/main/java/cc/shanruifeng/functions/udfs/scalar/geographic/README-geo.md)

### 7. china id card functions
| function| description |
|:--|:--|
|id_card_province(string) -> string |get user's province|
|id_card_city(string) -> string |get user's city|
|id_card_area(string) -> string |get user's area|
|id_card_birthday(string) -> string |get user's birthday|
|id_card_gender(string) -> string |get user's gender|
|is_valid_id_card(string) -> boolean |determine is valid china id card No.|
|id_card_info(string) -> json |get china id card info. include province, city, area etc.|

### 8. other functions
| function| description |
|:--|:--|
|is_null(all_type) -> boolean |whether is null or not|

## Use

put presto-third-functions-{version}-shaded.jar into `${presto_home}/plugin/hive-hadoop2` and restart presto server. for example:
### 1. restart presto
```
mv presto-third-functions-{version}-shaded.jar /home/presto/presto-server-0.147/plugin/hive-hadoop2/
cd /home/presto/presto-server-0.147
bin/launcher restart
```

### 2. set presto command
```
cd /home/presto/
ln -s presto-client-0.147/presto-cli-0.147-executable.jar presto-cli
export JAVA_HOME=/home/java8/jdk1.8.0_91/;
export PATH=/home/java8/jdk1.8.0_91/bin/:$PATH;
alias presto="/home/presto/presto-cli --server localhost:8080 --catalog hive --schema default"
```

### 3. example
#### 3.1 string functions
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

#### 3.2 array functions
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

#### 3.3 date functions
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

#### 3.4 JSON functions
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

#### 3.5 MAP functions
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

#### 3.6 china id card functions
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

#### 3.7 geographic functions
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

#### 3.8 other functions
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
