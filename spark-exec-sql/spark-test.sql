/**
  ./bin/spark-submit \
  /Users/pengdu/IdeaProjects/personal-project/exec-sql/spark-exec-sql/exec_spark_sql_file.py \
  -f /Users/pengdu/IdeaProjects/personal-project/exec-sql/spark-exec-sql/spark-test.sql \
  -df dt=2023-10-22
 */

-- 注释测试
SELECT ';' as a;
/* This is a comment
   contains ;*/ SELECT 1 as b;

SELECT 1 as c /* nested bracketed/*sss*/ comment*/;




select 'hello' /*hello world*/;

set var:a  = b; -- hie
-- add jar '/sss.jar'; -- jarjar
set var:c  = b; -- hie
-- set var:dt  = 2023-09-23; -- hie


select now(), '${dt}';

show databases;
-- create database hive_db;
use hive_db;
-- create table hive_t1(id int,name string);

-- insert into hive_t1 values(1001, 'spark-hive');

select * from hive_t1;

add jar '/Users/pengdu/Downloads/jars/paimon-spark-3.4-0.6.jar';

set spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog;
set spark.sql.catalog.paimon.warehouse=s3a://lakehouse/paimon/warehouse;
show databases ;
use paimon.test;
show tables;
select
    *
from paimon.test.t2;

/*
 end
 */