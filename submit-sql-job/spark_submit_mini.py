#! /usr/bin/env python
import argparse
import datetime
import os


def get_text_from_file(file_path):
    if os.path.exists(file_path):
        text = open(file_path, 'r').read()
        return text
    else:
        raise ValueError(f"SQL FILE '{file_path}' NOT FOUND.")


def add_kv_to_text(text, kv):
    if kv:
        for i in kv:
            var_key = i.split('=')[0]
            var_value = i.split('=')[1]
            text = text.replace('${' + f'{var_key}' + '}', str(var_value))
    return text


def get_conf_from_args():
    """
    从脚本参数获取 spark 任务运行配置
    :return:
    """
    conf_dict = {}
    conf_dict.setdefault("spark.master", args.master_url)
    conf_dict.setdefault("spark.submit.deployMode", args.deploy_mode)
    if not args.name:
        td = datetime.date.today().strftime('%Y%m%d')
        if args.exec_file:
            file_name = os.path.basename(args.exec_file).split('.')[0]
            app_name = '_'.join([file_name, td])
        elif args.query:
            app_name = '"' + args.query.splitlines()[0] + '"'
        else:
            raise ValueError("SQL Statement or SQL file not set.")
    else:
        app_name = args.name
    conf_dict.setdefault("spark.app.name", app_name)
    conf_dict.setdefault("spark.jars", args.jars)
    conf_dict.setdefault("spark.submit.pyFiles", args.py_files)
    conf_dict.setdefault("spark.driver.memory", args.driver_mem)
    conf_dict.setdefault("spark.driver.cores", args.driver_core)
    conf_dict.setdefault("spark.executor.memory", args.executor_mem)
    conf_dict.setdefault("spark.executor.cores", args.executor_core)
    conf_dict.setdefault("spark.executor.instances", args.num_executor)
    if args.conf:
        for conf in args.conf:
            conf_key = conf.split('=')[0]
            conf_value = conf.split('=')[1]
            conf_dict.setdefault(conf_key, conf_value)
    for key in list(conf_dict.keys()):
        if not conf_dict[key]:
            del conf_dict[key]
    return conf_dict


def generate_submit_command(conf_dict: dict, text, init_sql):
    """
    根据conf_dict和text生成spark-submit提交命令
    :param init_sql:
    :param conf_dict:
    :param text:
    :return: submit_command
    """
    if os.environ.get('SPARK_HOME'):
        spark_home = os.environ.get('SPARK_HOME')
    elif args.spark_home:
        spark_home = args.spark_home
    else:
        raise EnvironmentError("$SPARK_HOME not found in the environment variables and args ('--spark-home' or '-S').")
    conf_list = []
    for key, value in conf_dict.items():
        conf_list.append(f"\t--conf {key}={value}")
    conf_str = ' \\\n'.join(conf_list)
    if not args.exec_path:
        exec_path_dir = os.path.dirname(os.path.abspath(__file__))
        exec_path = os.path.join(exec_path_dir, 'exec_spark_sql.py')
    else:
        exec_path = args.exec_path
    command_lst = [f"{spark_home}/bin/spark-submit",
                   conf_str]
    if args.verbose:
        command_lst.append("\t--verbose")
    command_lst.append(f"\t{exec_path}")
    if init_sql:
        command_lst.append(f'\t-i "{init_sql}"')
    command_lst.append("\t--sql")
    command_lst.append(f'"{text}"')
    submit_command = ' \\\n'.join(command_lst)
    return submit_command


def handle_command(submit_command):
    pass


def main():
    conf_dict = get_conf_from_args()
    if args.exec_file:
        text = get_text_from_file(args.exec_file)
    elif args.query:
        text = args.query
    else:
        raise ValueError("SQL Statement or SQL file not set.")
    if args.init_sql:
        init_sql = get_text_from_file(args.init_sql)
    else:
        init_sql = None
    text_kv = add_kv_to_text(text, args.kv)
    submit_command = generate_submit_command(conf_dict, text_kv, init_sql)
    print("----------------> spark-submit command: \n" + submit_command)
    os.system(submit_command)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="提交 SPARK SQL 作业")
    parser.add_argument('--master', dest='master_url', default='local',
                        help='spark://host:port, mesos://host:port, yarn, k8s://https://host:port, or local (Default: local[*]).')
    parser.add_argument('--deploy-mode', default='client', dest='deploy_mode',
                        help='client or cluster(Default: client)')
    parser.add_argument('--name', dest='name', help='application name')
    parser.add_argument('--jars', dest='jars',
                        help='Comma-separated list of jars to include on the driver and executor classpaths.')
    parser.add_argument('--py-files', dest='py_files',
                        help='Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.')
    parser.add_argument('--conf', dest='conf', action='append', help='Arbitrary Spark configuration property.')
    parser.add_argument('--driver-memory', dest='driver_mem', default='1g',
                        help='Memory for driver (e.g. 1000M, 2G) (Default: 1g).')
    parser.add_argument('--driver-cores', dest='driver_core', default=1,
                        help='Number of cores used by the driver, only in cluster mode')
    parser.add_argument('--executor-memory', dest='executor_mem', default='1g',
                        help='Memory for executor (e.g. 1000M, 2G) (Default: 1g).')
    parser.add_argument('--executor-cores', dest='executor_core', help='Number of cores used by each executor.')
    parser.add_argument('--num-executors', dest='num_executor', default=1, help='Number of executors to launch (Default: 2).')
    parser.add_argument("--spark-home", '-S', dest='spark_home',
                        help='set SPARK_HOME path if Environment variable $SPARK_HOME not exists.')
    parser.add_argument('-x', dest="exec_path", help='path to exec_spark_sql.py')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-f", dest='exec_file', help='script file that should be executed.')
    group.add_argument("-e", dest='query', help='query that should be executed.')
    parser.add_argument('-i', dest='init_sql', help='Initialization SQL file')
    parser.add_argument('--define', '-d', dest='kv', action='append', help='Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B')
    parser.add_argument('--hivevar', dest='kv', action='append', help='Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B')
    # parser.add_argument('--silent', '-S',  dest='silent', action='store_true', help='Silent mode in interactive shell')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true', help='Verbose mode (echo executed SQL to the console)')
    args = parser.parse_args()
    main()
