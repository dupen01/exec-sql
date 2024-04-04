create table if Not exists catalog.db.t1 (
        id int comment'ID',
    name string comment ' 姓名',
    age int comment '年龄'
)   comment 't1表';

create table catalog.db.t2(
    id int comment 'ID'
    ,name string comment '姓名'
    ,age decimal(13,2) comment '年龄'
    ,dept varchar(200) comment '部门'
)comment 't2表';

create table catalog.db.t3(
                              id int comment 'ID'
    ,name string comment '姓名'
    ,age int comment ''
    ,dept varchar(200)
) ;
