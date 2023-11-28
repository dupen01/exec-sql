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


def exec_spark_sql(spark: SparkSession, sql_stmts, init_sql):
    if init_sql:
        for sql in init_sql:
            sql = re.sub(r'--.*', '', sql).strip()
            sql = re.sub(r'^/\*.*?\*/$', '', sql, flags=re.M | re.S).strip()
            if sql != '':
                try:
                    print(
                        f"=============================== [INIT-SQL] ===============================\n{sql}\n"
                        f"=============================== [INIT-SQL] ===============================")
                    spark.sql(sql)
                    print('OK')
                except Exception:
                    raise RuntimeError(f"Failed to initialize the SQL: '{sql}'.")
            else:
                continue
    sql_id = 0
    for sql in sql_stmts:
        sql = re.sub(r'--.*', '', sql).strip()
        sql = re.sub(r'^/\*.*?\*/$', '', sql, flags=re.M | re.S).strip()
        if sql == '':
            continue
        else:
            sql_id += 1
            print(
                f"=============================== [SQL-{sql_id}] ===============================\n{sql}\n"
                f"=============================== [SQL-{sql_id}] ===============================")
            spark.sql(sql).show()
    spark.stop()


def main():
    spark = create_spark_session()
    if args.query:
        # sql_text = args.query
        sql_stmts = parse_sql_text(args.query)
        if args.init_sql:
            init_sql = parse_sql_text(args.init_sql)
        else:
            init_sql = None
        exec_spark_sql(spark, sql_stmts, init_sql)
    else:
        raise ValueError("SQL Statement not find.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="执行 SPARK SQL 作业")
    # group = parser.add_mutually_exclusive_group()
    parser.add_argument("--sql", "-e", dest='query', help='query that should be executed.')
    parser.add_argument("--init", "-i", dest='init_sql', help='Initialization SQL script.')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='设置sql文本内的变量值，如 -d A=B or --define A=B')
    args = parser.parse_args()
    main()
