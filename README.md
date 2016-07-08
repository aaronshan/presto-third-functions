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
|dayofweek(date_string \| date)|计算给定日期是每周7天内的第几天,其中周一返回1,周天返回7,错误返回-1.|
|pinyin(string) | 将汉字转为拼音|
|zodiac(date_string \| date) | 将日期转换为星座英文 |
|zodiac_cn(date_string \| date) | 将日期转换为星座中文 | 
|id_card_province(string)|由身份证号获取省份|
|id_card_city(string)|由身份证号获取城市|
|id_card_area(string)|由身份证号获取区或县|
|id_card_birthday(string)|由身份证号获取出生日期|
|id_card_gender(string)|由身份证号获取性别|
|is_valid_id_card(string)|鉴别是否是有效的身份证号|


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

```