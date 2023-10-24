set 'pipeline.name' = 'flink-ck-on-s3-demo';
set 'execution.checkpointing.interval' = '10s';
set 'state.backend' = 'hashmap';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '3';
set 'state.checkpoints.dir' = 's3a://flink/ckp';


create table t1 (
    id int,
    name string,
    ts AS localtimestamp
)with(
    'connector' = 'datagen',
 'rows-per-second'='2',
 'fields.id.kind'='sequence',
 'fields.id.start'='1',
 'fields.id.end'='1000',
 'fields.name.length'='10'
);

create table t2 (
    id int,
    name string,
    ts timestamp
)with(
  'connector' = 'filesystem',           -- 必选：指定连接器类型
  'path' = 's3a://flink/tmp/t2',  -- 必选,指定路径
  'format' = 'json',
  'auto-compaction' = 'true'
);

insert into t2
select * from t1;

