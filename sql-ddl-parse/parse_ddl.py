import csv
import re
import sys

sql_file = './test-ddl.sql'
# sql_file = sys.argv[1]

target_file = 'dlink_table_dict.csv'


def get_field_info():
    ddl_statements = open(sql_file, 'r', encoding='utf-8').read()
    ddl_statements_lst = ddl_statements.split(';')
    for ddl in ddl_statements_lst:
        ddl = ddl.strip()
        if len(ddl) > 0:
            if ddl.upper().startswith('CREATE TABLE'):
                ddl = re.sub(r'\s+', ' ', ddl).lower().replace('if not exists', '')
                fields = re.search(r'\(.*\)', ddl).group()
                ddl_without_field = ddl.replace(fields, '')
                table_name = re.sub(r'comment.*', '', ddl_without_field).replace('create table', '').strip()
                table_comment_match = re.search(r'(?<=comment).*', ddl_without_field)
                table_comment = ''
                if table_comment_match:
                    table_comment = table_comment_match.group().replace("'", '').strip()
                fields = re.sub(r'^\(', '', re.sub(r'\)$', '', fields)).strip()
                # fields = fields.replace("comment'", "comment '")
                field_lst = re.split(r'(?<!\d),', re.sub(r'[ ](?=,)', '', fields))
                for field_info in field_lst:
                    field_info = field_info.strip()
                    # field_info_lst = field_info.split(' ')
                    field_info_lst = field_info.split('comment')
                    field_name = field_info_lst[0].split(' ')[0]
                    field_type = field_info_lst[0].split(' ')[1]
                    field_comment = ''
                    if len(field_info_lst) > 1:
                        field_comment = field_info_lst[1].replace("'", '').strip()
                    print(table_name, table_comment, field_name, field_comment, field_type)
                    yield table_name, table_comment, field_name, field_comment, field_type


# 写入到csv文件
with open(target_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['表名', '表描述', '字段名', '字段描述', '字段类型'])
    for row in get_field_info():
        writer.writerow(row)
    print(f'写入到文件{target_file} 成功。')