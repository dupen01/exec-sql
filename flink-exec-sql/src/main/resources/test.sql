set var: a  = b; -- hie
-- add jar '/sss.jar';
set table.sql-dialect = default; -- 123
set var:c  = b; -- hie
set var:dt  = 2023-09-23; -- hie

set table.sql-dialect = default;

select now(), '${dt}', '${x}';
-- ss;


set table.sql-dialect = default;
select '现在是default方言';

-- set table.sql-dialect = hive;
-- select '现在是hive方言';

-- select now(), '${dt}';

-- nihe;
/*

 select err
 */

select 1123,
       '${a}';