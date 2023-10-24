# flink-exec

#### 介绍
一个简单的脚本：读取SQL文件或文本并执行
支持在SQL文件内使用`${key1} ${key2}`来定义一个或多个变量，并在使用脚本时传参数`-df key1=value1 -df key2=value2`

- 支持多条`add jar '/path/to/your-jar.jar';` 语法来添加依赖包；
- 支持 `set table.sql-dialect = default ｜ hive;` 来切换Flink SQL 方言；
- 支持 `set var:your_key = your_value;` 自定义变量， 
    如在SQL文本内定义一个日期变量${dt}, 可以通过在SQL文本中配置 `set var:dt = 2023-09-23;`,
    也可以通过外部参数`-df dt=2023-09-23`来定义变量的值。
    若同时在SQL文本内和外部参数都配置了相同的key值，则SQL文本内的配置具有优先级。

#### 使用说明

脚本使用方法：
`exec_flink_sql_file.py -f|--file your_sqlfile.sql [-df dt=20220101 -df A=B]`
`exec_flink_sql_file.py --sql "some SQL statements" [-df dt=20220101 -df A=B]`

提交Flink任务使用`flink run --py exec_flink_sql_file.py [-f xxx.sql] [-df A=B]`

可以在SQL文件内添加`SET`语句来配置Flink任务运行时的参数，如
```sql
set 'pipeline.name' = 'kafka2iceberg';
set 'parallelism.default' = '1';
set 'rest.flamegraph.enabled' = 'true';
set 'pipeline.operator-chaining' = 'true';
set 'taskmanager.memory.managed.size' = '0mb';
set 'taskmanager.memory.managed.fraction' = '0.01';
set 'taskmanager.numberOfTaskSlots' =  '1';
set 'state.backend' =  'hashmap';
set 'execution.checkpointing.interval' =  '1min';
set 'state.checkpoints.dir' =  'hdfs://ns1/ckps';

```

# spark-exec

理论上可以支持spark所有支持的文件系统
如 file://  hdfs://  s3:// s3a:// oss:// 等
是 spark-sql 脚本的增强版（原生脚本仅支持从本地 file:// 获取sql文件且只能使用`client`模式）
本脚本支持从远程文件系统获取要执行的SQL文件，可以使用`cluster`模式提交作业

## 1. 定义文本内的变量：
使用 `set var:key = value ;`
```sql
set var:dt  = 2023-09-23;
select now(), '${dt}';
```

## 2. 注意注释：
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
select 2 as b /*可以在任;意位置*/; /*段内注释3*/
```
3. 支持嵌套的段内注释
```sql
/* This is a /* nested bracketed comment*/ .*/
SELECT 1 as c;
```

