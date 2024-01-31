import ftplib
import os


# 定义一个用于FTP操作的类
class FtpClient:
    def __init__(self, host, user, password, port=21):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.ftp = ftplib.FTP()

    # 连接FTP服务器
    def connect(self):
        try:
            self.ftp.connect(self.host, self.port)
            self.ftp.login(self.user, self.password)
            print(f"Connected to FTP server: {self.host}")
        except ftplib.all_errors as e:
            print(f"Failed to connect: {e}")

    # 断开FTP连接
    def disconnect(self):
        self.ftp.quit()
        print("Disconnected from FTP server.")

    # 上传文件
    def upload_file(self, local_file_path, remote_dir):
        try:
            with open(local_file_path, 'rb') as file:
                self.ftp.cwd(remote_dir)  # 更改远程工作目录
                filename = os.path.basename(local_file_path)
                self.ftp.storbinary('STOR ' + filename, file)
                print(f"Uploaded file {filename} to {remote_dir}")
        except Exception as e:
            print(f"Error uploading file: {e}")

    # 下载文件
    def download_file(self, remote_file_path, local_dir):
        try:
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            local_file = os.path.join(local_dir, os.path.basename(remote_file_path))

            with open(local_file, 'wb') as file:
                self.ftp.retrbinary('RETR ' + remote_file_path, file.write)
                print(f"Downloaded file {remote_file_path} to {local_file}")
        except ftplib.error_perm as e:
            print(f"Permission error or file not found: {e}")
        except Exception as e:
            print(f"Error downloading file: {e}")

    # 列出目录下的文件（显示文件名）
    def list_files(self, remote_dir="/"):
        try:
            self.ftp.cwd(remote_dir)
            files = self.ftp.nlst()
            # print(self.ftp.retrlines('NLST'))
            files = self.ftp.mlsd(path='', facts=['type', 'size'])
            print(f"Files in directory {remote_dir}:")
            for file in files:
                print(file[0], '---', file[1]['size'], '---', file[1]['type'])
        except ftplib.error_perm:
            print(f"Could not list files in directory {remote_dir}")


# 使用实例
client = FtpClient('127.0.0.1', 'dp', '123456', 2121)
client.connect()

client.list_files('/')
# client.upload_file('/Users/pengdu/IdeaProjects/personal-project/exec-sql/README.md', '/')
# client.download_file('/path/to/remote/file.txt', '/path/to/local/directory')
client.disconnect()
