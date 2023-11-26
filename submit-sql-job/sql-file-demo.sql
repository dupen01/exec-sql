set 'pipeline.name' = 'kafka2doris-realtime-data-operation-log';
set 'execution.checkpointing.interval' = '10s';
set 'state.backend' = 'hashmap';
set 'execution.checkpointing.tolerable-failed-checkpoints' = '3';
set 'state.checkpoints.dir' = 's3a://flink/ckp';




CREATE TABLE pg_dim(
                       station_id  BIGINT,
                       company_id BIGINT,
                       device_name STRING
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:postgresql://pgm-2ze04y52e6jputaq9o.pg.rds.aliyuncs.com:5432/shutan',
   'table-name' = 'heat_device_component',
   'username' = 'wangruitong',
   'password' = '8QPXHURQdqFBat',
   'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.expire-after-write' = '24h',
    'lookup.partial-cache.max-rows' = '2000'
);

CREATE TABLE realtime_tbl(
                             `topic` STRING,
                             `reportOn` BIGINT,
                             `reportData` STRING,
                             proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'realtime-data',
  'properties.bootstrap.servers' = '39.107.228.248:9092',
  'properties.group.id' = 'shutan01',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE log_tbl(
                        `topic` STRING,
                        `report_on` BIGINT,
                        `payload` STRING,
                        `clientid` STRING,
                        proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'operation-log',
  'properties.bootstrap.servers' = '39.107.228.248:9092',
  'properties.group.id' = 'flink_operation_log01',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE doris_log_sink (
                                station_id int,
                                topic string,
                                payload string,
                                client_id string,
                                report_on bigint,
                                report_date string,
                                updated_on bigint,
                                dt string,
                                pdt date
)
WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.188.112:8030',
    'table.identifier' = 'heating.kafka_operation_log',
    'username' = 'etl_u',
    'password' = 'shutan123',
    'sink.label-prefix' = 'kafka2DorisLog03'
);


CREATE TABLE doris_realtime_sink (
                                     report_on bigint,
                                     report_date varchar,
                                     company_id bigint,
                                     station_id bigint,
                                     updated_on bigint,
                                     bt double,
                                     bp double,
                                     pt double,
                                     pp double,
                                     cl double,
                                     ca double,
                                     zd double,
                                     ph double,
                                     `do` double,
                                     el double,
                                     hd double,
                                     hop double,
                                     hip double,
                                     cip double,
                                     cop double,
                                     hit double,
                                     hot double,
                                     cit double,
                                     cot double,
                                     dt varchar,
                                     pdt date
)
WITH (
    'connector' = 'doris',
    'fenodes' = '192.168.188.112:8030',
    'table.identifier' = 'heating.kafka_realtime_data',
    'username' = 'etl_u',
    'password' = 'shutan123',
    'sink.label-prefix' = 'kafka2DorisRealtime03'
);

insert into doris_log_sink
select
    cast(pg_dim.station_id as int) as station_id,
    log_tbl.topic as topic,
    log_tbl.payload as payload,
    log_tbl.clientid as client_id,
    log_tbl.report_on as report_on,
    FROM_UNIXTIME( log_tbl.report_on / 1000 , 'yyyy-MM-dd HH:mm:ss') as report_date,
    UNIX_TIMESTAMP() as updated_on,
    FROM_UNIXTIME(log_tbl.report_on / 1000, 'yyyyMMdd') as dt,
    to_date(FROM_UNIXTIME(log_tbl.report_on / 1000, 'yyyy-MM-dd')) as pdt
from log_tbl
         left join pg_dim
    FOR SYSTEM_TIME AS OF log_tbl.proc_time
on pg_dim.device_name = SUBSTRING(log_tbl.topic,3);

insert into doris_realtime_sink
select
    realtime_tbl.reportOn as report_on,
    FROM_UNIXTIME( realtime_tbl.reportOn / 1000 , 'yyyy-MM-dd HH:mm:ss') as report_date,
    pg_dim.company_id as company_id,
    pg_dim.station_id as station_id,
    UNIX_TIMESTAMP() as updated_on,
    cast(json_value(realtime_tbl.reportData, '$.bt') as double),
    cast(json_value(realtime_tbl.reportData, '$.bp') as double),
    cast(json_value(realtime_tbl.reportData, '$.pt') as double),
    cast(json_value(realtime_tbl.reportData, '$.pp') as double),
    cast(json_value(realtime_tbl.reportData, '$.cl') as double),
    cast(json_value(realtime_tbl.reportData, '$.ca') as double),
    cast(json_value(realtime_tbl.reportData, '$.zd') as double),
    cast(json_value(realtime_tbl.reportData, '$.ph') as double),
    cast(json_value(realtime_tbl.reportData, '$.do') as double),
    cast(json_value(realtime_tbl.reportData, '$.el') as double),
    cast(json_value(realtime_tbl.reportData, '$.hd') as double),
    cast(json_value(realtime_tbl.reportData, '$.hop') as double),
    cast(json_value(realtime_tbl.reportData, '$.hip') as double),
    cast(json_value(realtime_tbl.reportData, '$.cip') as double),
    cast(json_value(realtime_tbl.reportData, '$.cop') as double),
    cast(json_value(realtime_tbl.reportData, '$.hit') as double),
    cast(json_value(realtime_tbl.reportData, '$.hot') as double),
    cast(json_value(realtime_tbl.reportData, '$.cit') as double),
    cast(json_value(realtime_tbl.reportData, '$.cot') as double),
    FROM_UNIXTIME(realtime_tbl.reportOn / 1000, 'yyyyMMdd') as dt,
    to_date(FROM_UNIXTIME(realtime_tbl.reportOn / 1000, 'yyyy-MM-dd')) as pdt
from realtime_tbl
         left join pg_dim
    FOR SYSTEM_TIME AS OF realtime_tbl.proc_time
on pg_dim.device_name = SUBSTRING(realtime_tbl.topic,3);