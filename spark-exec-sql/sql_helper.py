from typing import List

from pyspark.sql import SparkSession

sql_script = """
/*
sele d;
/*
*/

*/
set var: a = 'hello', b= 999 ;
select ${a}, ${b} ;
select '/*d;*/'
*
from s; -- de ;d;
select /*de/*de */  de */ now()
    , a /*hello;/*de*/ ;你好*/ 
    , b -- xss ;  
    , c -- xxx
    , 'aa; bb'
from t ; select  --- -- de ;
x
, y
, z
from b;  -- oooooo
select ';',';cs--'; select 789;
select now(); select 123; select 456 
, ${b}; select ';gj', a from t  ; select /*ab;cd*/';cs--'; select /*+ coalesce(1) */ 789; -- ' hu' ;  
"""


# sql_script = """
# select 123;select 4; -- ab;
# select
# *
# from t ;
# """


def get_variable_from_line(variable_list: List, line: str):
    """
    从一行sql中获取自定义变量，并添加到一个列表中
    """
    SQL_VARIABLE_PREFIX = "set var:"
    line = line.strip().lower()
    if line.startswith(SQL_VARIABLE_PREFIX):
        for map in line.replace(SQL_VARIABLE_PREFIX, '')[:-1].split(','):
            k = map.split("=")[0].strip()
            v = map.split("=")[1].strip()
            variable_list.append((k, v))
        return ''
    return line


def replace_variable(variable_list: List[tuple], line: str):
    if len(variable_list) > 0:
        for kv in variable_list:
            line = line.replace('${' + f'{kv[0]}' + '}', kv[1])
    return line


def split_sql_script_to_statements(sql_script: str) -> List[str]:
    sql_list = []
    # variable_list = []
    # 嵌套注释的层级数
    multi_comment_level = 0
    prefix = ""
    for line in sql_script.splitlines():
        # update variable_list
        # line = get_variable_from_line(variable_list, line)
        # update line with variable_list
        # line = replace_variable(variable_list, line)
        # 标记是否以双引号结尾
        has_terminated_double_quote = True
        # 标记是否以单引号结尾
        has_terminated_single_quote = True
        # 标记是否属于单行注释内容
        is_single_line_comment = False
        # 标记前一个字符是否是短横行 "-"
        was_pre_dash = False
        # 标记前一个字符是否是斜杆 "/"
        was_pre_slash = False
        # 标记前一个字符是否是星号 "*"
        was_pre_star = False
        last_semi_index = 0
        index = 0
        if len(prefix) > 0:
            prefix += "\n"
        # for index in range(len(line)):
        for char in line:
            # match list(line)[index]:
            match char:
                case "'":
                    if has_terminated_double_quote:
                        has_terminated_single_quote = not has_terminated_single_quote
                case '"':
                    if has_terminated_single_quote:
                        has_terminated_double_quote = not has_terminated_double_quote
                case '-':
                    if has_terminated_double_quote and has_terminated_single_quote:
                        if was_pre_dash:
                            is_single_line_comment = True
                    was_pre_dash = True
                case '/':
                    if has_terminated_double_quote and has_terminated_single_quote:
                        # 如果'/'前面是'*'， 那么嵌套层级数-1
                        if was_pre_star:
                            multi_comment_level -= 1

                    was_pre_slash = True
                    was_pre_dash = False
                    was_pre_star = False
                case '*':
                    if has_terminated_double_quote and has_terminated_single_quote:
                        # 如果'*'前面是'/'， 那么嵌套层级数+1
                        if was_pre_slash:
                            multi_comment_level += 1
                    was_pre_star = True
                    was_pre_dash = False
                    was_pre_slash = False
                case ';':
                    # 当分号不在单引号内，不在双引号内，不属于单行注释，并且多行嵌套注释的层级数为0时，表示此分号应该作为分隔符进行划分
                    if (has_terminated_double_quote and
                            has_terminated_single_quote and
                            not is_single_line_comment and
                            multi_comment_level == 0):
                        # sql_list.append(line[last_semi_index:index + 1])
                        sql_list.append(prefix + line[last_semi_index:index])
                        prefix = ""
                        last_semi_index = index + 1
                    # else:
                        # line = line + " "
                case _:
                    was_pre_dash = False
                    was_pre_slash = False
                    was_pre_star = False
            index += 1
        if last_semi_index != index or len(line) == 0:
            # sql_list.append(line[last_semi_index:])
            prefix = prefix + line[last_semi_index:]
    assert multi_comment_level == 0, (f"The number of nested levels of sql multi-line comments is not equal to 0: "
                                      f"{multi_comment_level}")
    # return "\n".join(sql_list).split(";\n")
    return sql_list


sql_statement = split_sql_script_to_statements(sql_script)
sql_id = 1
for sql in sql_statement:
    print(sql_id, sql)
    sql_id += 1
