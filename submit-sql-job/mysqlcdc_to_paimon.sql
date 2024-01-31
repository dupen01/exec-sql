add jar '/Users/pengdu/Library/app/flink-1.17.2/usrlib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar';
add jar '/Users/pengdu/Library/app/flink-1.17.2/usrlib/flink-sql-connector-mysql-cdc-2.4.2.jar';
add jar '/Users/pengdu/Library/app/flink-1.17.2/usrlib/paimon-flink-1.17-0.7-20231202.001918-12.jar';
-- add jar 'file:///Users/pengdu/Library/app/flink-1.17.2/usrlib/paimon-s3-0.7-20231202.001918-13.jar';

set 'pipeline.name' = 'mysqlcdc2paimon';

-- checkpoint配置
set 'state.backend.type' = 'hashmap';
set 'execution.checkpointing.interval' = '10s';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '3';
set 'state.checkpoints.dir' = 's3://flink/ckps';


-- 创建mysql cdc source 表
create table t1(
    id int,
    name string,
    ts timestamp
)with(
  'connector' = 'mysql-cdc',
  'scan.startup.mode' = 'earliest-offset',
  'hostname' = '172.20.3.11',
  'port' = '3306',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'test',
  'scan.incremental.snapshot.chunk.key-column' = 'id',-- 非主键表需要
  'table-name' = 't1'
);
-- 创建paimon catalog
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = 's3://lakehouse/paimon/warehouse',
    'lock.enabled' = 'true',
    's3.endpoint' = 'http://172.20.3.12:9000',
    's3.access-key' = 'minio',
    's3.secret-key' = '12345678'
);


EXECUTE STATEMENT SET
BEGIN
insert into paimon.test.t2
select
    id,
    name,
    date_format(ts,'yyyy-MM-dd')
  from t1;
insert into paimon.test.t1
select * from paimon.test.t2;
END;