import time
import datetime
from contextlib import contextmanager


@contextmanager
def get_run_time():
    """
    自定义装饰器，实现统计with runtime_ts 语法内的代码的执行时间
    :return:
    """
    start_ts = time.time()
    start_time = datetime.datetime.now()
    yield None
    end_ts = time.time()
    end_time = datetime.datetime.now()
    print("+------------- 任务运行时间 --------------+")
    print("|任务开始时间:", start_time, '|')
    print("|任务结束时间:", end_time, '|')
    print("|运行时间(ms):", str((end_ts - start_ts) * 1000), '\t\t|')
    print("|运行时间(s) :", str(end_ts - start_ts), '\t\t|')
    print("+---------------------------------------+")


def test():
    time.sleep(3)


if __name__ == '__main__':
    with get_run_time():
        test()
