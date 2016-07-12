# presto-third-functions

## Introduction

some presto functions

## Build
```
cd ${project_home}
mvn clean package
```

It will generate presto-third-functions-0.1.0-shaded.jar in target directory.

## functions
| function| description |
|:--|:--|
|dayofweek(date_string \| date) -> int | day of week,if monday,return 1, sunday return 7, error return -1.|
|pinyin(string) -> int | convert chinese to pinyin|
|zodiac(date_string \| date) -> string | convert date to zodiac|
|zodiac_cn(date_string \| date) -> string | convert date to zodiac chinese | 
|wgs_distance(double lat1, double lng1, double lat2, double lng2) -> double | calculate WGS84 coordinate distance, in meters|
|md5(string) -> string | md5 hash|
|sha256(string) -> string |sha256 hash|

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
```


