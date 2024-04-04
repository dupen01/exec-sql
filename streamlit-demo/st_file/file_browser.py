import time

import streamlit as st
import os

class FileInfo:
    def __init__(self, name, mtime, size, ftype):
        self.name = name
        self.mtime = mtime
        self.size = size
        self.ftype = ftype


class FileReader:
    def __init__(self, work_dir="."):
        self.work_dir = work_dir

    def get_files(self, file_path):
        files = []
        fullname = os.path.join(self.work_dir, file_path)
        if os.path.isdir(fullname):
            for name in sorted(os.listdir(fullname), key=lambda x: x.lower()):
                child_fullname = os.path.join(fullname, name)
                file_type = '文件夹'
                file_size = '--'
                _time = time.strftime("%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(child_fullname)))
                if os.path.isfile(child_fullname):
                    file_size = self.__get_file_size(child_fullname)
                    file_type = os.path.splitext(child_fullname)[1][1:]
                if not name.startswith('.'):
                    files.append(FileInfo(name, _time, file_size, file_type))
        return files

    @staticmethod
    def __get_file_size(file_name):
        size = os.path.getsize(file_name)
        if size < 1024:
            return f"{size} 字节"
        elif 1024 <= size < 1024 * 1024:
            return f"{round(size / 1024, 2)} KB"
        elif 1024 <= size < 1024 * 1024 * 1024:
            return f"{round(size / (1024 * 1024), 2)} MB"
        else:
            return f"{round(size / (1024 * 1024 * 1024), 2)} GB"


def upload_files(file_path):
    uploaded_files = st.file_uploader("上传文件", accept_multiple_files=True)
    for uploaded_file in uploaded_files:
        bytes_data = uploaded_file.read()
        with open(os.path.join(file_path, uploaded_file.name), 'wb') as f:
            f.write(bytes_data)
        st.write("filename:", uploaded_file.name)
        st.write(bytes_data)


def show_files(file_path="."):
    if os.path.exists(file_path):
        if os.path.isdir(file_path):
            fr = FileReader()
            files = fr.get_files(file_path)
            for file in files:
                bt = st.button(label=file.name)
                if bt:
                    show_files(os.path.join(file_path, file.name))
                # st.markdown(f"""
                # - {file.name} | {file.mtime} | {file.size} | {file.ftype}
                # """)
        else:
            with open(file_path) as file:
                file_name = os.path.split(file_path)[1]
                st.download_button(
                    label=file_name,
                    file_name=file_name,
                    data=file
                )
    else:
        raise FileExistsError(f"file path not exists:{file_path}")


upload_files("./")
show_files("./")
