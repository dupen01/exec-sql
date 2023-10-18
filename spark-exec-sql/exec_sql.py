"""
usage: python exec_sql.py spark|flink -sf /sql/file/path [-df k1=v1 -df k2=v2]
sql语句内的变量格式 ${value}
required arguments:
    spark | flink       指定计算引擎
    -sf, --sql-file     传入SQL文件路径，支持本地路径 file://, /local/path/ 或远程文件系统如 hdfs://, s3://
    -st, --sql-text     传入SQL文本，字符串类型，如 "SELECT * FROM t"
note:-sf与-st是互斥参数，只能二选一

optional arguments:
    -df, --define       设置sql语句内的变量值，如 -df dt=20220101 -df A=B
"""
import argparse
import re
import logging
import os
from typing import List


def read_from_flink(file_path: str):
    from pyflink.datastream import StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    if os.path.exists(file_path):
        return open(file_path, 'r').read()
    return "\n".join(env.read_text_file(file_path).execute_and_collect(limit=9999))


def read_from_spark(file_path: str):
    from pyspark import SparkContext
    if os.path.exists(file_path):
        return open(file_path, 'r').read()
    return "\n".join(SparkContext().textFile(file_path, 1).collect())


def parse_sql_text(raw_sql_text: str, kv=None) -> List[str]:
    if kv:
        for k, v in kv.items():
            raw_sql_text = raw_sql_text.replace('${' + f'{k}' + '}', str(v))
    if raw_sql_text:
        # sql_without_annotation = re.sub(r'^\s*/\*.*?\*/$', '', raw_sql_text, flags=re.M | re.S)
        sql_lines = raw_sql_text.splitlines()
        sql_stmt = []
        for line in sql_lines:
            if not line.strip().startswith("--"):
                sql_stmt.append(line)
        return re.split(r';\s*$|;(?=\s*\n)|;(?=\s*--)', "\n".join(sql_stmt))


def exec_spark_sql_text(sql_text: str, kv=None):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sql_stmts = parse_sql_text(sql_text, kv)
    for sql in sql_stmts:
        sql = sql.strip()
        if sql == '':
            continue
        else:
            logging.warning(f"-------------- SQL STATEMENT -------------\n{sql};")
            spark.sql(sql).show()


def exec_flink_sql_text(sql_text: str, kv=None):
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.common import Configuration
    sql_stmts = parse_sql_text(sql_text, kv)
    # 读取SQL文件内的SET语句，并进行环境配置
    config = Configuration()
    jars_path_lst = []
    for sql in sql_stmts:
        sql = sql.strip()
        if sql:
            logging.warning(f"-------------- SQL STATEMENT -------------\n{sql};")
        if sql.upper().startswith('SET'):
            set_key = re.findall(r'(?<=[sS][eE][tT] ).*(?= *\=)', sql.replace('\'', ''))[0].strip()
            set_value = re.findall(r'(?<=\=).*', sql.replace('\'', ''))[0].strip()
            config.set_string(set_key, set_value)
        elif sql.upper().startswith('ADD JAR'):
            jar_path = re.findall(r"(?<=').*(?=')", sql.replace('"', '\''))[0].strip()
            jars_path_lst.append(jar_path)
    if len(jars_path_lst) > 0:
        jars_path = ';'.join(jars_path_lst)
        config.set_string('pipeline.jars', jars_path)
    env_settings = EnvironmentSettings \
        .new_instance() \
        .in_streaming_mode() \
        .with_configuration(config) \
        .build()
    t_env = TableEnvironment.create(env_settings)
    statement_set = t_env.create_statement_set()
    statement_set_flag = False
    for sql in sql_stmts:
        sql = sql.strip()
        if sql == '' or sql.upper().startswith('SET') or sql.upper().startswith('ADD'):
            continue
        else:
            if sql.upper().startswith('INSERT'):
                statement_set.add_insert_sql(sql)
                statement_set_flag = True
            else:
                t_env.execute_sql(sql).print()
    if statement_set_flag:
        statement_set.execute()


def main():
    parser = argparse.ArgumentParser(description="执行Flink SQL 文件或文本")
    group = parser.add_mutually_exclusive_group()
    parser.add_argument('compute', help='compute engine: spark or flink')
    group.add_argument('--sql-file', '-sf', dest='sql_file',
                       help='传入SQL文件路径，支持本地路径 file://, /local/path/ 或远程文件系统如 hdfs://, s3://')
    group.add_argument('--sql-text', '-st', dest='sql_text', help='传入SQL文本')
    parser.add_argument('--define', '-df',dest='kv', action='append',
                        help='设置sql语句内的变量值，如 -df dt=20220101 -df A=B')
    args = parser.parse_args()
    kv = {}
    if args.kv:
        for i in args.kv:
            kk = i.split('=')
            kv.setdefault(kk[0], kk[1])
    if args.compute is None or args.compute.upper() not in ['SPARK', 'FLINK']:
        raise ValueError(f"The parameter is invalid. It should be spark or flink:{args.compute}")
    else:
        logging.warning(f'============== {args.compute.upper()} ENGINE =====================')
        if args.compute.upper() == 'SPARK':
            if args.sql_file:
                sql_text = read_from_spark(args.sql_file)
                exec_spark_sql_text(sql_text, kv)
            elif args.sql_text:
                exec_spark_sql_text(args.sql_text, kv)
            else:
                raise ValueError('SQL FILE OR SQL TEXT NOT FOUND')
        elif args.compute.upper() == 'FLINK':
            if args.sql_file:
                sql_text = read_from_flink(args.sql_file)
                exec_flink_sql_text(sql_text, kv)
            elif args.sql_text:
                exec_flink_sql_text(args.sql_text, kv)
            else:
                raise ValueError('SQL FILE OR SQL TEXT NOT FOUND')


if __name__ == '__main__':
    main()
