"""
/KYUUBI_HOME/bin/beeline \
-u "jdbc:hive2://<ip>:<port>/<database>;#<conf1>;<conf2>..." \
-n <username> \
-p <password> \
-f <sql_script> \

jdbc:hive2://172.20.3.16:10009;#k1=v1;k2=v2


封装一个脚本，参数类似spark-submit，灵活配置spark运行时配置，如内存、cpu等，底层调用kyuubi-beeline脚本来执行sql文件
"""
import argparse
import datetime
import logging
import os


def get_logger(log_file=None):
    _logger = logging.getLogger('test-name')
    _logger.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)
    _logger.addHandler(console_handler)
    return _logger


logger = get_logger()


def generate_beeline_command(username, password, sql_script, jdbc_url):
    BEELINE_PATH = "/Users/pengdu/Library/app/kyuubi-1.8.0/bin/beeline"

    beeline_command_lst = [BEELINE_PATH]
    beeline_command_lst.append(f'-u "{jdbc_url}"')
    beeline_command_lst.append(f"-n {username}")
    beeline_command_lst.append(f"-p {password}")
    beeline_command_lst.append(f"-f {sql_script}")
    beeline_command = " \\\n".join(beeline_command_lst)
    return beeline_command


def get_jdbc_url_from_properties(host, port, spark_properties: list = None):
    """
    jdbc:hive2://172.20.3.16:10009/;#kyuubi.engine.share.level=CONNECTION;spark.master=local
    """
    jdbc_prefix = "jdbc:hive2://"
    jdbc_suffix = '/;#'
    jdbc_url = jdbc_prefix + host + ":" + str(port)
    if spark_properties:
        properties_str = ";".join(spark_properties)
        return jdbc_url + jdbc_suffix + properties_str
    return jdbc_url


def get_spark_configurations_from_args():
    spark_configurations = []
    share_level = args.share_level.upper()
    if share_level:
        spark_configurations.append(f"kyuubi.engine.share.level={share_level}")
    spark_configurations.append(f"spark.master={args.master_url}")
    spark_configurations.append(f"spark.submit.deployMode={args.deploy_mode}")
    if args.jars:
        spark_configurations.append(f'spark.jars={args.jars}')
    if args.py_files:
        spark_configurations.append(f'spark.submit.pyFiles={args.py_files}')
    spark_configurations.append(f'spark.driver.memory={args.driver_mem}')
    spark_configurations.append(f'spark.driver.cores={args.driver_core}')
    spark_configurations.append(f'spark.executor.memory={args.executor_mem}')
    if args.executor_core:
        spark_configurations.append(f'spark.executor.cores={args.executor_core}')
    spark_configurations.append(f'spark.executor.instances={args.num_executor}')
    if args.spark_conf:
        for conf in args.spark_conf:
            spark_configurations.append(conf)
    if args.master_url.upper() == 'YARN':
        spark_configurations.append(f'spark.yarn.queue={args.queue}')
    return spark_configurations


def show_properties(spark_properties: list):
    for conf in spark_properties:
        key = conf.split('=')[0]
        value = conf.split('=')[1]
        logger.info(f"{'-' * 20} {key}:{value}")
    return


def hide_jdbc_password(beeline_command: str):
    command_lst = []
    for line in beeline_command.splitlines():
        if line.startswith('-p'):
            replaced_line = "-p ****** \\"
            command_lst.append(replaced_line)
        else:
            command_lst.append(line)
    return "\n".join(command_lst)




def main():
    host = "172.20.3.16"
    port = 10009
    spark_configurations = get_spark_configurations_from_args()
    show_properties(spark_configurations)
    jdbc_url = get_jdbc_url_from_properties(host, port, spark_configurations)
    username = args.username
    password = args.password
    if os.path.exists(args.exec_file):
        sql_script = os.path.abspath(args.exec_file)
    else:
        raise FileNotFoundError(f"SQL FILE '{args.exec_file}' NOT FOUND.")
    beeline_command = generate_beeline_command(username, password, sql_script, jdbc_url)
    beeline_command_out = hide_jdbc_password(beeline_command)
    logger.info(f"Launching Kyuubi Beeline command ->\n{beeline_command_out}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage="python %(prog)s [spark-submit options] ...")
    parser.add_argument('--engine-share-level', '-esl', dest='share_level',
                        choices=['connection', 'user', 'group', 'server'], help='kyuubi.engine.share.level')
    parser.add_argument('--master', dest='master_url', default='local',
                        help='spark://host:port, mesos://host:port, yarn, k8s://https://host:port, or local (Default: local[*]).')
    parser.add_argument('--deploy-mode', default='client', dest='deploy_mode',
                        help='client or cluster(Default: client)')
    parser.add_argument('--name', dest='name', help='application name')
    parser.add_argument('--jars', dest='jars',
                        help='Comma-separated list of jars to include on the driver and executor classpaths.')
    parser.add_argument('--py-files', dest='py_files',
                        help='Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.')
    parser.add_argument('--conf', '-c', dest='spark_conf', action='append',
                        help='Arbitrary Spark configuration property.')
    parser.add_argument('--driver-memory', dest='driver_mem', default='1g',
                        help='Memory for driver (e.g. 1000M, 2G) (Default: 1g).')
    parser.add_argument('--driver-cores', dest='driver_core', default=1,
                        help='Number of cores used by the driver, only in cluster mode')
    parser.add_argument('--executor-memory', dest='executor_mem', default='1g',
                        help='Memory for executor (e.g. 1000M, 2G) (Default: 1g).')
    parser.add_argument('--executor-cores', dest='executor_core',
                        help='Number of cores used by each executor.')
    parser.add_argument('--num-executors', dest='num_executor', default=1,
                        help='Number of executors to launch (Default: 1).')
    parser.add_argument('--proxy-user', dest='user',
                        help='User to impersonate when submitting the application. (Default: "default").')
    parser.add_argument('--queue', dest='queue', default='default',
                        help='The YARN queue to submit to (Default: "default").')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", dest='exec_file', help='script file that should be executed.')
    group.add_argument("-e", dest='query', help='query that should be executed.')
    parser.add_argument('-i', dest='init_sql', help='Initialization SQL file')

    parser.add_argument('-n', dest='username', required=True, help='the username to connect as')
    parser.add_argument('-p', dest='password', required=True, help='the password to connect as')
    parser.add_argument('--log-file', '-lf', dest='log_file', help='log file path')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B')
    parser.add_argument('--hivevar', dest='kv', action='append',
                        help='Variable substitution to apply to Hive commands. e.g. -d A=B or --hivevar A=B')
    args = parser.parse_args()
    main()
