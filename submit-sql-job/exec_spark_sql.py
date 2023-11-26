import argparse
import re
from typing import List

from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()


def parse_sql_text(text) -> List[str]:
    sql_lines = text.splitlines()
    sql_stmts = []
    for line in sql_lines:
        sql_stmts.append(line)
    return re.split(r';\s*$|;(?=\s*\n)|;(?=\s*--)', "\n".join(sql_stmts))


def exec_spark_sql(spark: SparkSession, sql_stmts):
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


def main():
    spark = create_spark_session()
    if args.query:
        sql_text = args.query
        sql_stmts = parse_sql_text(sql_text)
        exec_spark_sql(spark, sql_stmts)
    else:
        raise ValueError("SQL Statement not find.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="执行 SPARK SQL 作业")
    # group = parser.add_mutually_exclusive_group()
    parser.add_argument("-e", dest='query', help='query that should be executed.')
    parser.add_argument('--define', '-d', dest='kv', action='append', help='设置sql文本内的变量值，如 -d A=B or --define A=B')
    args = parser.parse_args()
    main()
