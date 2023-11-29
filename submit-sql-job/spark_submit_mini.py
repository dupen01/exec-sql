#! /usr/bin/env python
import argparse
import datetime
import logging
import os
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s - %(message)s')


def get_text_from_file(file_path):
    if os.path.exists(file_path):
        text = open(file_path, 'r').read()
        return text
    else:
        raise FileNotFoundError(f"SQL FILE '{file_path}' NOT FOUND.")


def add_kv_to_text(text, kv=None):
    if kv:
        for i in kv:
            var_key = i.split('=')[0]
            var_value = i.split('=')[1]
            text = text.replace('${' + f'{var_key}' + '}', str(var_value))
    return text


def generate_submit_command(exec_sql, init_sql=None):
    if os.environ.get('SPARK_HOME'):
        spark_home = os.environ.get('SPARK_HOME')
    elif args.spark_home:
        spark_home = args.spark_home
    else:
        raise EnvironmentError(
            "$SPARK_HOME not found in the environment variables nor in the parameters ('--spark-home' or '-S').")
    command_lst = [f"{spark_home}/bin/spark-submit"]
    command_lst.append(f'--master {args.master_url}')
    command_lst.append(f'--deploy-mode {args.deploy_mode}')
    if not args.name:
        td = datetime.date.today().strftime('%Y%m%d')
        if args.exec_file:
            file_name = os.path.basename(args.exec_file).split('.')[0]
            app_name = '_'.join([file_name, td])
        else:
            app_name = '"' + args.query.splitlines()[0] + '"'
    else:
        app_name = args.name
    command_lst.append(f'--name {app_name}')
    if args.jars:
        command_lst.append(f'--jars {args.jars}')
    if args.py_files:
        command_lst.append(f'--py-files {args.py_files}')
    command_lst.append(f'--driver-memory {args.driver_mem}')
    command_lst.append(f'--driver-cores {args.driver_core}')
    command_lst.append(f'--executor-memory {args.executor_mem}')
    if args.executor_core:
        command_lst.append(f'--executor-cores {args.executor_core}')
    command_lst.append(f'--num-executors {args.num_executor}')
    if args.spark_conf:
        for conf in args.spark_conf:
            command_lst.append(f'--conf {conf}')
    if args.verbose:
        command_lst.append('--verbose')
    if args.queue and args.master_url.upper() == 'YARN':
        command_lst.append(f'--queue {args.queue}')
    command_lst.append(f'--proxy-user {args.user}')
    if not args.exec_path:
        exec_path_dir = os.path.dirname(os.path.abspath(__file__))
        exec_path = os.path.join(exec_path_dir, 'exec_spark_sql.py')
    else:
        exec_path = args.exec_path
    command_lst.append(exec_path)
    if init_sql:
        command_lst.append('--init')
        command_lst.append(f'"{init_sql}"')
    command_lst.append('--sql')
    command_lst.append(f'"{exec_sql}"')
    submit_command = ' \\\n  '.join(command_lst)
    return submit_command


def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output_log = []
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            logging.info(output.strip())
            output_log.append(output.strip())
    return_code = process.poll()
    if return_code != 0:
        logging.error(f"Spark job failed with return code: {return_code}")
        sys.exit(1)
    else:
        logging.info(f"return code: {return_code}")
    return str(output_log)


def handle_log(log):
    pass


def main():
    if args.exec_file:
        exec_sql = get_text_from_file(args.exec_file)
    else:
        exec_sql = args.query
    if args.init_sql:
        init_sql = get_text_from_file(args.init_sql)
    else:
        init_sql = None
    exec_sql_kv = add_kv_to_text(exec_sql, args.kv)
    submit_command = generate_submit_command(exec_sql_kv, init_sql)
    logging.info("Submit Spark SQL job:\n" + submit_command)
    run_command(submit_command)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage="python %(prog)s [spark-submit options] [-S SPARK_HOME] [-x exec_spark_sql.py] [-f SQL file]")
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
    parser.add_argument('--proxy-user', dest='user', default='default',
                        help='User to impersonate when submitting the application. (Default: "default").')
    parser.add_argument('--queue', dest='queue', default='default',
                        help='The YARN queue to submit to (Default: "default").')
    parser.add_argument("--spark-home", '-S', dest='spark_home',
                        help='set SPARK_HOME path if Environment variable $SPARK_HOME not exists.')
    parser.add_argument('-x', dest="exec_path", help='path to exec_spark_sql.py')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", dest='exec_file', help='script file that should be executed.')
    group.add_argument("-e", dest='query', help='query that should be executed.')
    parser.add_argument('-i', dest='init_sql', help='Initialization SQL file')
    parser.add_argument('--define', '-d', dest='kv', action='append',
                        help='Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B')
    parser.add_argument('--hivevar', dest='kv', action='append',
                        help='Variable substitution to apply to Hive commands. e.g. -d A=B or --hivevar A=B')
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true',
                        help='Verbose mode (echo executed SQL to the console)')
    args = parser.parse_args()
    main()
