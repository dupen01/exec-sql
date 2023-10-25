import argparse
import re
import os
from typing import List
from pyspark.sql import SparkSession

sql_separator_regex = r';\s*$|;(?=\s*\n)|;(?=\s*--)'

# 获取 spark 执行环境(测试使用，使用spark-submit提交作业时需要删掉或注释掉)
if not os.environ.get('SPARK_HOME'):
    os.environ.setdefault('SPARK_HOME', "/Users/pengdu/Library/app/spark-3.4.1-bin-hadoop3")

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

print("""
                       ____                   _      ____   ___  _     
   _____  _____  ___  / ___| _ __   __ _ _ __| | __ / ___| / _ \| |    
  / _ \ \/ / _ \/ __| \___ \| '_ \ / _` | '__| |/ / \___ \| | | | |    
 |  __/>  <  __/ (__   ___) | |_) | (_| | |  |   <   ___) | |_| | |___ 
  \___/_/\_\___|\___| |____/| .__/ \__,_|_|  |_|\_\ |____/ \__\_\_____|
                            |_|                                       
""")


def get_sql_text_from_file(file_path):
    schema = file_path.split("://")[0]
    if os.path.exists(file_path):
        print("将从本地文件系统读取文件...")
        sql_text = open(file_path, 'r').read()
    else:
        print(f"将从远程文件系统 '{schema}' 读取文件...")
        sql_text = "\n".join(spark.sparkContext.textFile(file_path, 1).collect())
    return sql_text


# 解析SQL文本
def parse_sql_text(sql_text, kv=None) -> List[str]:
    sql_line_lst = sql_text.splitlines()
    for line in sql_line_lst:
        line = re.sub(r'\s+', ' ', line)
        if line.upper().startswith("SET VAR:"):
            var_map = re.sub(r';.*', "", re.sub(r'set\s+var:', '', line))
            var_key = var_map.split("=")[0].strip()
            var_value = var_map.split("=")[1].strip()
            sql_text = sql_text.replace('${' + f'{var_key}' + '}', str(var_value))
            print(f"SQL文件定义的变量值: {var_key}={var_value}")
    # 将dict类型的kv变量放入sql
    if kv:
        for i in kv:
            var_key = i.split('=')[0]
            var_value = i.split('=')[1]
            sql_text = sql_text.replace('${' + f'{var_key}' + '}', str(var_value))
            print(f"外部参数定义的变量值: {var_key}={var_value}")
    sql_lines = sql_text.splitlines()
    sql_stmts = []
    for line in sql_lines:
        sql_stmts.append(line)
    return re.split(sql_separator_regex, "\n".join(sql_stmts))


def exec_sql_text(sql_stmts):
    sql_id = 0
    for sql in sql_stmts:
        sql = re.sub(r'--.*', '', sql).strip()
        sql = re.sub(r'^/\*.*?\*/$', '', sql, flags=re.M | re.S).strip()
        if sql == '' or sql.upper().startswith("SET VAR:"):
            continue
        else:
            sql_id += 1
            print(
                f"-------------- [SQL-{sql_id} start] -------------\n{sql}\n"
                f"-------------- [SQL-{sql_id}   end] -------------")
            spark.sql(sql).show()
    spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="执行 SPARK SQL 文件或文本")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-f", dest='sql_file', help='SQl from files')
    group.add_argument("-e", dest='sql_text', help='SQL from command line')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='设置sql文本内的变量值，如 -d A=B or --define A=B')
    args = parser.parse_args()
    if args.sql_file:
        parsed_sql_text = get_sql_text_from_file(args.sql_file)
        exec_sql_text(parse_sql_text(parsed_sql_text, args.kv))
    elif args.sql_text:
        exec_sql_text(parse_sql_text(args.sql_text, args.kv))
    else:
        raise ValueError('SQL FILE NOT FOUND.')
