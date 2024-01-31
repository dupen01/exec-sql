import ftplib

import streamlit as st



st.title('FTP Server')
st.write('---')


# def list_files(dir='/'):
#     ftp.cwd(dir)
#     files = ftp.mlsd(facts=['size', 'type'])
#     for file in files:
#         button = st.button(file[0])
#         if not file[0].startswith('.'):
#             if file[1]['type'] == 'dir':
#                 ftp.cwd(file[0])


st.download_button('download', 'hello upload')
st.file_uploader('upload', 'hello upload')
