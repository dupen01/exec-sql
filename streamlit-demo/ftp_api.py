import ftplib

import uvicorn
from fastapi import FastAPI

app = FastAPI()

ftp = ftplib.FTP()
host = '127.0.0.1'
port = 2121
user = 'dp'
passwd = '123456'
ftp.connect(host, port)
ftp.login(user, passwd)
print(f'Connected to FTP server: {host} with user: {user}')


@app.get('/{file_path}')
def list_files(file_path):
    ftp.cwd(file_path)
    files = ftp.mlsd(facts=['size', 'type'])
    for file in files:
        if not file[0].startswith('.'):
            if file[1]['type'] == 'dir':
                list_files(file[0])


@app.get('/{file_full_path}')
def download(file_full_path):
    return {
        "msg": "200",
        "data": f"这是文件，直接下载:{file_full_path}"
    }


def ls(file_path):
    ftp.cwd(file_path)
    files = ftp.mlsd(facts=['size', 'type'])
    for file in files:
        if not file[0].startswith('.'):
            if file[1]['type'] == 'dir':
                list_files(file[0])
            else:
                download(file[0])


@app.get('/getMe')
def get_me():
    return {
        "Name": "D&P",
        "Tickle": "小肚腩"
    }


if __name__ == '__main__':
    uvicorn.run('ftp_api:app', host='127.0.0.1', port=8080, reload=True)
