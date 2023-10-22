"""
理论上可以支持spark所有支持的文件系统
如 file://  hdfs://  s3:// s3a:// oss:// 等
是 spark-sql 脚本的增强版（原生脚本仅支持从本地 file:// 获取sql文件且只能使用`client`模式）
本脚本支持从远程文件系统获取要执行的SQL文件，可以使用`cluster`模式提交作业
"""

from pyspark import SparkContext
import argparse
import re
import os
from typing import List
from pyspark.sql import SparkSession

sql_separator_regex = r';\s*$|;(?=\s*\n)|;(?=\s*--)'

# 获取 spark 执行环境(测试使用，使用spark-submit提交作业时需要删掉或注释)
spark_home = "/Users/pengdu/Library/app/spark-3.4.1-bin-hadoop3"
if not os.environ.get('SPARK_HOME'):
    os.environ.setdefault('SPARK_HOME', spark_home)

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

print("""
                       ____                   _                _ 
   _____  _____  ___  / ___| _ __   __ _ _ __| | __  ___  __ _| |
  / _ \ \/ / _ \/ __| \___ \| '_ \ / _` | '__| |/ / / __|/ _` | |
 |  __/>  <  __/ (__   ___) | |_) | (_| | |  |   <  \__ \ (_| | |
  \___/_/\_\___|\___| |____/| .__/ \__,_|_|  |_|\_\ |___/\__, |_|
                            |_|                             |_|  
""")

def get_sql_text_from_file(file_path):
    if os.path.exists(file_path):
        print("将从本地文件系统读取文件...")
        sql_text = open(file_path, 'r').read()
    else:
        print("将从远程文件系统读取文件...")
        sql_text = "\n".join(SparkContext().textFile(file_path, 1).collect())
    return sql_text


# 解析SQL文本
def parse_sql_text(sql_text: str, kv=None) -> List[str]:
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
    # 删除文本内的段注释内容，如 /* 段注释 */
    # sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.M | re.S)
    sql_lines = sql_text.splitlines()
    sql_stmts = []
    for line in sql_lines:
        sql_stmts.append(line)
    return re.split(sql_separator_regex, "\n".join(sql_stmts))


def exec_sql_text(sql_stmts):
    sql_id = 0
    for sql in sql_stmts:
        sql = re.sub(r'--.*', '', sql).strip()
        sql = re.sub(r'^/\*.*?\*/$', '', sql, flags=re.M | re.S).strip( )
        if sql == '' or sql.upper().startswith("SET VAR:"):
            continue
        else:
            sql_id += 1
            print(
                f"-------------- [SQL-{sql_id} start] -------------\n{sql}\n"
                f"-------------- [SQL-{sql_id}   end] -------------")
            spark.sql(sql).show()
    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="执行 SPARK SQL 文件或文本")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-f", dest='sql_file', help='SQl from files')
    group.add_argument("-e", dest='sql_text', help='SQL from command line')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='设置sql文本内的变量值，如 -d A=B or --define A=B')
    args = parser.parse_args()
    if args.sql_file:
        sql_text = get_sql_text_from_file(args.sql_file)
        exec_sql_text(parse_sql_text(sql_text, args.kv))
    elif args.sql_text:
        exec_sql_text(parse_sql_text(args.sql_text, args.kv))
    else:
        raise ValueError('SQL FILE NOT FOUND')


if __name__ == '__main__':
    main()
