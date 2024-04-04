create table dim_district as
select * from read_csv('/Users/pengdu/IdeaProjects/personal-project/exec-sql/holidays/district.csv', AUTO_DETECT=TRUE,header=True);



select
    t2.id as province_code
     , t2.name as provice_name
     , cast(if(t3.id is null, concat(substring(t1.id,0,5),'00'), t3.id) as int) as city_code
     , if(t3.id is null, t2.name, t3.name) as city_name
     , t1.id
     , t1.name
from main.dim_district t1
         left join (
    select
        id,name
    from main.dim_district
    where ends_with(id,'0000')
)t2 on substring(t1.id,0,3) = substring(t2.id,0,3)
         left join (
    select
        id,name
    from main.dim_district
    where ends_with(id,'00') and not ends_with(id,'0000')
)t3 on substring(t1.id,0,5) = substring(t3.id,0,5)
where not ends_with(t1.id,'00')
order by t1.id