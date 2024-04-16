show databases ;
select
    now(),
    current_date,
    year(current_date),
    current_timestamp
;

-- select count(1) from paimon.hms.t1;

-- 快照表
-- select * from paimon.hms.`t1$snapshots`;




-- master change
-- master change2