import argparse
import logging
import re
from typing import List

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(filename)s - %(message)s')


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
    app_id = spark.sparkContext.applicationId
    logging.info(f"Application ID: {app_id}")
    if init_sql:
        for sql in init_sql:
            sql = re.sub(r'--.*', '', sql).strip()
            sql = re.sub(r'^/\*.*?\*/$', '', sql, flags=re.M | re.S).strip()
            if sql != '':
                try:
                    logging.info("=============================== [INIT-SQL] ===============================")
                    logging.info(sql)
                    logging.info("=============================== [INIT-SQL] ===============================")
                    spark.sql(sql)
                    logging.info('OK')
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
            logging.info(f"=============================== [SQL-{sql_id}] ===============================")
            logging.info(sql)
            logging.info(f"=============================== [SQL-{sql_id}] ===============================")
            spark.sql(sql).show()
    spark.stop()


def main():
    spark = create_spark_session()
    if args.query:
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
    parser.add_argument("--sql", "-e", dest='query', help='query that should be executed.')
    parser.add_argument("--init", "-i", dest='init_sql', help='Initialization SQL script.')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='设置sql文本内的变量值，如 -d A=B or --define A=B')
    args = parser.parse_args()
    main()
