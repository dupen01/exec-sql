import argparse
import os
import subprocess
import sys


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
    if os.environ.get('FLINK_HOME'):
        flink_home = os.environ.get('FLINK_HOME')
    elif args.flink_home:
        flink_home = args.flink_home
    else:
        raise EnvironmentError("FLINK_HOME not found in the environment variables nor in the parameters ('--flink-home' or '-F').")
    command_lst = [f"{flink_home}/bin/flink"]
    if args.target in run_targets:
        command_lst.append("run")
    else:
        command_lst.append("run-application")
    command_lst.append(f'--target {args.target}')
    if args.class_name:
        command_lst.append(f'--class {args.class_name}')
    if args.classpath:
        command_lst.append(f'--classpath {args.classpath}')
    if args.detached:
        command_lst.append('--detached')
    if args.parallelism:
        command_lst.append(f'--parallelism {args.parallelism}')
    if args.restoreMode:
        command_lst.append(f'--restoreMode {args.restoreMode}')
    if args.savepoint:
        command_lst.append(f'--fromSavepoint {args.savepoint}')
    if args.shutdownOnAttachedExit:
        command_lst.append('--shutdownOnAttachedExit')
    if args.allowNonRestoredState:
        command_lst.append('--allowNonRestoredState')
    if args.properties:
        for prop in args.properties:
            command_lst.append(f'-D{prop}')
    command_lst.append(args.job_jar)
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
            print(output.replace('\n', ''))
            output_log.append(output.replace('\n', ''))
    return_code = process.poll()
    if return_code != 0:
        print(f"Flink job failed with return code: {return_code}")
        sys.exit(1)
    else:
        print(f"return code: {return_code}")
    return str(output_log)


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
    print("Submit Flink SQL job:\n" + submit_command)
    run_command(submit_command)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    run_targets = ['remote', 'kubernetes-session', 'yarn-session', 'yarn-per-job', 'local']
    application_targets = ['kubernetes-application', 'yarn-application']
    public_parser = argparse.ArgumentParser(add_help=False)
    public_parser.add_argument('--class', '-c', dest='class_name', help='')
    public_parser.add_argument('--classpath', '-C', dest='classpath', help='')
    public_parser.add_argument('--detached', '-d', dest='detached', action='store_true', help='')
    public_parser.add_argument('--allowNonRestoredState', '-n', dest='allowNonRestoredState', action='store_true',
                               help='')
    public_parser.add_argument('--parallelism', '-p', dest='parallelism', help='')
    public_parser.add_argument('--restoreMode', '-rm', dest='restoreMode', help='')
    public_parser.add_argument('--fromSavepoint', '-s', dest='savepoint', help='')
    public_parser.add_argument('--shutdownOnAttachedExit', '-sae', dest='shutdownOnAttachedExit', action='store_true',
                               help='')
    public_parser.add_argument('-D', dest='properties', action='append', help='')
    public_parser.add_argument('--flink-home', '-F', dest='flink_home')
    public_parser.add_argument('-x', dest="job_jar", required=True, help='path to job jar')
    group = public_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-f", dest='exec_file', help='script file that should be executed.')
    group.add_argument("-e", dest='query', help='query that should be executed.')
    public_parser.add_argument('-i', dest='init_sql', help='Initialization SQL file')
    public_parser.add_argument('--define', '-df', dest='kv', action='append',
                               help='Variable substitution to apply to Hive commands. e.g. -d A=B or --define A=B')
    sub = parser.add_subparsers()
    run = sub.add_parser('run', parents=[public_parser], help='run options')
    application = sub.add_parser('run-application', parents=[public_parser], help='run-application options')
    run.add_argument('--target', '-t', dest='target', choices=run_targets, required=True)
    application.add_argument('-target', '-t', dest='target', choices=application_targets, required=True,
                             help='application-mode')
    args = parser.parse_args()
    main()
