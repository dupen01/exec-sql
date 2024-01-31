"""
python -m pyftpdlib -udp -P123456
-i 指定IP地址，默认本机IP
-p 指定端口，默认2121
-w 读写权限，默认只读
-d 指定目录，默认为当前目录
-u 指定用户名
-P 设置登陆密码
"""

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import argparse


def start_ftp_server():
    # 创建一个授权器实例
    authorizer = DummyAuthorizer()
    # 添加一个用户
    authorizer.add_user(args.user, args.password, args.file_path, perm='elrmw')

    # 创建FTP处理器实例，并关联到授权器
    handler = FTPHandler
    handler.authorizer = authorizer

    # 设置FTP服务器监听的地址和端口
    server = FTPServer(('0.0.0.0', args.port), handler)

    # 开启FTP服务器
    server.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(usage="python %(prog)s ")
    parser.add_argument('--user', '-u', default='public', dest='user')
    parser.add_argument('--password', '-P', default='123456', dest='password')
    parser.add_argument('--file-path', '-d', default='./', dest='file_path')
    parser.add_argument('--port', '-p', default='2121', dest='port')
    args = parser.parse_args()
    start_ftp_server()
