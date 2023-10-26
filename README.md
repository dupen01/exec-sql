# 1. flink-exec

## 1.1 介绍
一个简单的jar包：读取 Flink SQL 脚本并执行。

支持在SQL文件内使用`${key}`来定义一个或多个变量，并在使用脚本时传参数`-d key1=value1 -d key2=value2`

- 支持多条`add jar '/path/to/your-jar.jar';` 添加依赖包，用法同Flink SQL Client；
- 支持 `set table.sql-dialect = default ｜ hive;` 来切换Flink SQL 方言，用法同Flink SQL Client；
- 支持 `set var:your_key = your_value;` 来自定义变量， 
    如在SQL文本内定义一个日期变量${dt}, 可以通过在SQL文本中配置 `set var:dt = 2023-09-23;`,
    也可以通过外部参数`-d dt=2023-09-23`来定义变量的值。
    若同时在SQL文本内和外部参数都配置了相同的key值，则SQL文本内的配置具有优先级。

## 1.2 使用说明

使用方法，与提交 Flink jar 包作业一致。如：
```shell
./bin/flink run \
-c com.dupeng.flink.sql.ExecFlinkSqlFile \
/path/to/flink-exec-sql-1.0-SNAPSHOT_*.jar \
-f /path/to/sql-file.sql \
-d dt=20231025 \
--define a=hello-flink

./bin/flink run \
-c com.dupeng.flink.sql.ExecFlinkSqlFile \
/path/to/flink-exec-sql-1.0-SNAPSHOT_*.jar \
-f hdfs://ns/path/to/sql-file.sql \
-d dt=20231025 \
--define a=hello-flink

# 使用带main方法入口的jar包提交作业，不需指定-c main方法入口
./bin/flink run \
-Ds3.access-key=7HAP3WcpW6fq70bWbEJW \
-Ds3.secret-key=YWG7hREHDsuXfj8IsEPG8CQP92iBSE4vlWQu9Xbj \
-Ds3.endpoint=http://localhost:9000 \
/path/to/flink-exec-sql-1.0-SNAPSHOT_*-main.jar \
-f s3://bk-test/test.sql \
-d x=20237788
```



可以在SQL脚本内添加`SET`语句来配置Flink任务运行时的参数，如
```sql
-- 配置运行时参数
set 'parallelism.default' = '1';
set 'execution.checkpointing.interval' =  '1min';
set 'state.checkpoints.dir' = 'hdfs://ns1/ckps';
-- 添加依赖包
add jar '/path/to/some-jar.jar';
-- 设置变量
set var: a = b;
-- 切换 Flink SQL 方言
set table.sql-dialect = hive;

-- 多条 insert 语句
insert into t2
select * from t1;

insert into t3
select * from t1;
```

## 1.3 关于SQL注释
一般情况下，对注释没有特别规定，你可以按照自己习惯的SQL注释编写SQL脚本。
目前已知的特别情况应该避免出现：
在`/*多行注释*/`内，某一段注释以分号`;`结尾的，代码会按照这个分号`;`将SQL语句划分为多段，这将导致不正确的SQL语句。
例如：
```sql
/*
 这是一段多行注释
 这是一段多行注释;
 */
select 'hello';
```
以上SQL脚本会被划分为
```sql
/*
 这是一段多行注释
 这是一段多行注释;
```
和
```sql
*/
select 'hello';
```
这将导致执行失败。
所以应该避免在多行注释中使用分号`;`结尾。

# 2. spark-exec
是 `spark-sql` 脚本的增强版（原生脚本仅支持从本地 file:// 获取sql文件且只能使用`client`模式提交SQL作业）。

本脚本支持从远程文件系统获取要执行的SQL文件，可以使用`cluster`模式提交作业。

理论上可以支持spark所有支持的文件系统
如 `file://`、`hdfs://`、`s3://`、`s3a://`、`oss://` 等。需要自行解决相关依赖。

## 2.1. 定义文本内的变量：
使用 `set var:key = value ;`
```sql
set var:dt  = 2023-09-23;
select now(), '${dt}';
```

## 2.2. 注意注释：
一般来说、本程序对于注释的使用有着比较不错的容错性，比如支持：
1. 单行的注释，如 
```sql
-- 单行注释
select 1 as a;
```
2. 段内注释，如
```sql
/*段内 
  注释*/
select 2/*可以在任;意位置*/ as b ; /*段内注释3*/
```
3. 支持嵌套的段内注释
```sql
/* This is a /* nested bracketed comment*/ .*/
SELECT 1 as c;
```

## 2.3 提交 Spark 作业
```shell
# 若以 cluster 模式提交作业，你的SQL脚本应该置于远处文件系统如 hdfs, s3, oss 等
./bin/spark-submit \
--master yarn \
--deploy-mode cluster \
/path/to/exec_spark_sql_file.py \
-f hdfs://ns/some-sql.sql \
-d dt=20231025 \
--define a=hello-spark 
```

