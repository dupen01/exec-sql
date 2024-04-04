import argparse
import logging
import re
from typing import List
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.session import SparkContext


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(filename)s - %(message)s')


def create_spark_session():
    return SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()


def parse_sql_text(text) -> List[str]:
    """
    过时的：since 2024-01-29
    :param text:
    :return:
    """
    sql_lines = text.splitlines()
    sql_stmts = []
    for line in sql_lines:
        sql_stmts.append(line)
    return re.split(r';\s*$|;(?=\s*\n)|;(?=\s*--)', "\n".join(sql_stmts))


def __get_variable_from_line(variable_list: List, line: str):
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


def __replace_variable(variable_list: List[tuple], line: str):
    if len(variable_list) > 0:
        for kv in variable_list:
            line = line.replace('${' + f'{kv[0]}' + '}', kv[1])
    return line


def split_sql_script_to_statements(sql_script: str) -> List[str]:
    sql_list = []
    # 嵌套注释的层级数
    multi_comment_level = 0
    prefix = ""
    for line in sql_script.splitlines():
        # if line.strip().startswith('--'):
        line = line if not line.strip().startswith('--') else ''
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
        for char in line:
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
                        sql_stmt = prefix + line[last_semi_index:index]
                        sql_list.append(sql_stmt)
                        prefix = ""
                        last_semi_index = index + 1
                case _:
                    was_pre_dash = False
                    was_pre_slash = False
                    was_pre_star = False
            index += 1
        if last_semi_index != index or len(line) == 0:
            prefix = prefix + line[last_semi_index:]
    assert multi_comment_level == 0, (f"The number of nested levels of sql multi-line comments is not equal to 0: "
                                      f"{multi_comment_level}")
    return sql_list


def get_application_info(sc: SparkContext):
    spark_conf = sc.getConf()
    driver_cores = spark_conf.get("spark.driver.cores") if spark_conf.get("spark.driver.cores") else 1
    driver_memory = spark_conf.get("spark.driver.memory") if spark_conf.get("spark.driver.memory") else '1g'
    executor_cores = spark_conf.get("spark.executor.cores") if spark_conf.get("spark.executor.cores") else 1
    executor_memory = spark_conf.get("spark.executor.memory") if spark_conf.get("spark.executor.memory") else '1g'
    executor_num = spark_conf.get("spark.executor.instances") if spark_conf.get("spark.executor.instances") else 1
    start_time = datetime.datetime.fromtimestamp(sc.startTime/1000)[:-3]
    application_info = f"""
    Spark application Name: {sc.appName}
          application ID: {sc.applicationId}
          application web UI: {sc.uiWebUrl}
          master: {sc.master}
          version: {sc.version}
          driver: [cpu:{driver_cores}, mem: {driver_memory}]
          executor: [cpu:{executor_cores}, mem: {executor_memory}, num: {executor_num}]
    User: {sc.sparkUser()}
    Start time: {start_time}
    """
    return application_info


def exec_spark_sql(spark: SparkSession, sql_stmts, init_sql):
    application_info = get_application_info(spark.sparkContext)
    logging.info(application_info)

    if init_sql:
        for sql in init_sql:
            logging.info(f"INIT-SQL: {sql}")
            if len(sql.strip()) > 0:
                try:
                    spark.sql(sql)
                    logging.info('OK')
                except Exception:
                    raise RuntimeError(f"Failed to initialize the SQL: '{sql}'.")
            else:
                continue
    sql_id = 0
    for sql in sql_stmts:
        if len(sql.strip()) > 0:
            sql_id += 1
            logging.info(f"sql_id-{sql_id} ->\n{sql}")
            try:
                spark.sql(sql).show()
            except Exception:
                raise RuntimeError(f"Failed to execute the SQL: '{sql}'.")
    spark.stop()


def main():
    spark = create_spark_session()
    if args.query:
        sql_stmts = split_sql_script_to_statements(args.query)
        if args.init_sql:
            init_sql = split_sql_script_to_statements(args.init_sql)
        else:
            init_sql = None
        exec_spark_sql(spark, sql_stmts, init_sql)
    else:
        raise ValueError("SQL Statement not find.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="执行 SPARK SQL 作业")
    parser.add_argument("--sql", "-e", dest='query', help='query that should be executed.')
    parser.add_argument("--init", "-i", dest='init_sql', help='Initialization SQL script.')
    # parser.add_argument('--define', '-d', dest='kv', action='append',
    #                     help='设置sql文本内的变量值，如 -d A=B or --define A=B')
    args = parser.parse_args()
    main()
