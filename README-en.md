# presto-third-functions

## Introduction

some presto functions

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
mvn clean install -DskipTests
```

It will generate presto-third-functions-0.1.0-shaded.jar in target directory.

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

### 3. date functions
| function| description |
|:--|:--|
|dayofweek(date_string \| date) -> int | day of week,if monday,return 1, sunday return 7, error return -1.|
|zodiac(date_string \| date) -> string | convert date to zodiac|
|zodiac_cn(date_string \| date) -> string | convert date to zodiac chinese | 

### 4. geographic functions 
| function| description |
|:--|:--|
|wgs_distance(double lat1, double lng1, double lat2, double lng2) -> double | calculate WGS84 coordinate distance, in meters|

### 5. other functions
| function| description |
|:--|:--|
|is_null(all_type) -> boolean |whether is null or not|

## Use

put presto-third-functions-0.1.0-shaded.jar into `${presto_home}/plugin/hive-hadoop2` and restart presto server. for example:
### 1. restart presto
```
mv presto-third-functions-0.1.0-shaded.jar /home/presto/presto-server-0.147/plugin/hive-hadoop2/
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

presto:default> select md5(col1), sha256(col1) from (values 'aaronshan') as t(col1)\G;
-[ RECORD 1 ]-----------------------------------------------------------
_col0 | 95686bc0483262afe170b550dd4544d1
_col1 | d16bb375433ad383169f911afdf45e209eabfcf047ba1faebdd8f6a0b39e0a32

Query 20160712_071936_00006_hkbes, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]


presto:default> select is_null(col0),is_null(col1),is_null(col2),is_null(col3) from (values ('test', 1, 0.5, ARRAY [1]),(null, null, null, null)) as t(col0, col1, col2,col3);
 _col0 | _col1 | _col2 | _col3
-------+-------+-------+-------
 false | false | false | false
 true  | true  | true  | true
(2 rows)

Query 20160713_061435_00003_82kmt, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]

presto:default> select array_union(arr1, arr2) from (values (ARRAY [1,3,5,null], ARRAY [2,3,4,null])) as t(arr1, arr2);
         _col0
-----------------------
 [1, 3, 5, null, 2, 4]
(1 row)

Query 20160713_061707_00004_82kmt, FINISHED, 1 node
Splits: 1 total, 0 done (0.00%)
0:00 [0 rows, 0B] [0 rows/s, 0B/s]
```


