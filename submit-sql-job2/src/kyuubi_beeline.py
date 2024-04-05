import argparse
import logging
import os
import subprocess
import sys

# BEELINE_PATH = "/Users/dupeng/Library/app/apache-kyuubi-1.9.0-bin/bin/beeline"
KYUUBI_HOME = "/Users/dupeng/Library/app/apache-kyuubi-1.9.0-bin"

# get logger
logger = logging.getLogger('test-name')
logger.setLevel(level=logging.INFO)
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
formatter = logging.Formatter('%(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


# 从命令行参数获取配置
def get_conf_from_args(args):
    spark_configurations = []
    share_level = args.share_level.upper()
    spark_configurations.append(f"kyuubi.engine.share.level={share_level}")
    if args.subdomain:
        spark_configurations.append(f"kyuubi.engine.share.level.subdomain={args.subdomain}")
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


def generate_jdbc_url(host, port, confs: list = None):
    """
    jdbc:hive2://172.20.3.16:10009/;#kyuubi.engine.share.level=CONNECTION;spark.master=local;...
    """
    jdbc_prefix = "jdbc:hive2://"
    jdbc_suffix = '/;#'
    jdbc_url = jdbc_prefix + host + ":" + str(port)
    if confs:
        properties_str = ";".join(confs)
        return jdbc_url + jdbc_suffix + properties_str
    return jdbc_url


# 生成 kyuubi command
def generate_beeline_command(username, password, sql_script, jdbc_url):
    if os.environ.get("KYUUBI_HOME"):
        kyuubi_home = os.environ.get("KYUUBI_HOME")

    else:
        kyuubi_home = KYUUBI_HOME
    if os.path.exists(kyuubi_home):
        beeline_path = os.path.join(kyuubi_home, "bin/beeline")
        beeline_command_lst = [beeline_path,
                               f'-u "{jdbc_url}"',
                               f"-n {username}",
                               f"-p {password}",
                               f"-f {sql_script}"]
        beeline_command = " \\\n".join(beeline_command_lst)
        return beeline_command
    else:
        raise FileNotFoundError(f"{kyuubi_home} not found")



# 运行command
def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            # 打印日志
            logger.info(output.replace('\n', ''))
    return_code = process.poll()
    if return_code != 0:
        logger.error(f"Execution failed with return code: {return_code}")
        sys.exit(1)
    else:
        logger.info(f"return code: {return_code}")


def show_properties(confs: list):
    for conf in confs:
        key = conf.split('=')[0]
        value = conf.split('=')[1]
        logger.info(f"{'-' * 20} {key}: {value}")
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


def print_sql_statement(args):
    sql_path = args.exec_file
    sql_statement = ''
    if sql_path:
        with open(os.path.abspath(sql_path), 'r') as f:
            sql_statement = f.read()
    logger.info(f"{'-' * 20} SQL statement:\n{sql_statement}")


def arg_parse():
    parser = argparse.ArgumentParser(
        usage="python %(prog)s [options] ...")
    parser.add_argument('--engine-share-level', '-esl', dest='share_level', default='user',
                        choices=['connection', 'user', 'group', 'server'],
                        help='configuration for `kyuubi.engine.share.level`')
    parser.add_argument('--engine-subdomain', '-es', dest='subdomain',
                        help='configuration for `kyuubi.engine.share.level.subdomain`')
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
                        help='Arbitrary Spark or Kyuubi configuration property.')
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
    parser.add_argument("-f", dest='exec_file', help='script file that should be executed.')
    parser.add_argument('-i', dest='init_sql', help='Initialization SQL file')

    parser.add_argument('-n', dest='username', required=True, help='the username to connect as')
    parser.add_argument('-p', dest='password', required=True, help='the password to connect as')
    parser.add_argument('--host', dest='host', default='127.0.0.1', help='the Kyuubi Server host to connect to')
    parser.add_argument('--port', dest='port', default='10009', help='the Kyuubi Server port to connect to')
    # parser.add_argument('--log-file', '-lf', dest='log_file', help='log file path')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B')
    parser.add_argument('--hivevar', dest='kv', action='append',
                        help='Variable substitution to apply to Hive commands. e.g. -d A=B or --hivevar A=B')
    args = parser.parse_args()
    return args


def main():
    args = arg_parse()
    confs = get_conf_from_args(args)
    jdbc_url = generate_jdbc_url(args.host, args.port, confs)
    username = args.username
    password = args.password
    if os.path.exists(args.exec_file):
        sql_script_path = os.path.abspath(args.exec_file)
    else:
        raise FileNotFoundError(f"SQL FILE '{args.exec_file}' NOT FOUND.")
    beeline_command = generate_beeline_command(username, password, sql_script_path, jdbc_url)
    beeline_command_out = hide_jdbc_password(beeline_command)
    logger.info(f"Launching Kyuubi Beeline command ->\n{beeline_command_out}")
    show_properties(confs)
    print_sql_statement(args)
    run_command(beeline_command)


if __name__ == '__main__':
    main()
