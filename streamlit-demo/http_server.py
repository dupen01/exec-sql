import os
import http.server

import uvicorn
from fastapi import FastAPI, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

app = FastAPI()


head_res = """<html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <title>Directory listing for /</title>
    </head>
    <body>
    <h1>Directory listing for /</h1>
    <hr>
    <br>
    <input type="file" name="file" id="fileInput" />
    <br>
    
    <script>
    /**
    * @Description: 上传提交
    * @date 2024/1/17
    */
    function uploadFile() {
    const fileInput = document.getElementById('fileInput');
    
    fileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/upload', true);
    xhr.onreadystatechange = function () {
    if (xhr.readyState === 4 && xhr.status === 200) {
    console.log('上传成功');
    }
    };
    
    const formData = new FormData();
    formData.append('file', file);
    xhr.send(formData);
    })
    }
    
    /**
    * @Description: 加载完成后执行
    * @date 2024/1/17
    */
    window.onload = () => {
    uploadFile()
    }
    </script>
    <ul>
"""

end_res = """</ul>
    <hr>
    </body>
    </html>
"""

error_res = """
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html;charset=utf-8">
        <title>Error response</title>
    </head>
    <body>
        <h1>Error response</h1>
        <p>Error code: 404</p>
        <p>Message: File not found.</p>
        <p>Error code explanation: HTTPStatus.NOT_FOUND - Nothing matches the given URI.</p>
    </body>
</html>
"""

@app.get("/test")
def test():
    t = """
    <!DOCTYPE html>
<html lang="en">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>Directory listing for /</title>
</head>
<body>
<h1>Directory listing for /</h1>
<hr>
<br>
<input type="file" name="file" id="fileInput" />
<br>

<script>
/**
* @Description: 上传提交
* @date 2024/1/17
*/
function uploadFile() {
const fileInput = document.getElementById('fileInput');

fileInput.addEventListener('change', (e) => {
const file = e.target.files[0];
const xhr = new XMLHttpRequest();
xhr.open('POST', '/upload', true);
xhr.onreadystatechange = function () {
if (xhr.readyState === 4 && xhr.status === 200) {
console.log('上传成功');
}
};

const formData = new FormData();
formData.append('file', file);
xhr.send(formData);
})
}

/**
* @Description: 加载完成后执行
* @date 2024/1/17
*/
window.onload = () => {
uploadFile()
}
</script>
</body>
</html>
    """
    # print(t.format(test='tttt'))
    return HTMLResponse(t)

@app.post("/upload")
async def upload_file(file: UploadFile):
    """
    文件上传到当前工作目录
    """
    with open(file.filename, 'wb') as f:
        f.write(file.file.read())
    return {"filename": file.filename}


@app.get("/{file_path:path}")
def read_file(file_path):
    fullname = os.path.join(os.getcwd(), file_path)
    res = [head_res]
    if os.path.exists(fullname):
        if os.path.isdir(fullname):
            files = os.listdir(fullname)
            for file in files:
                child_fullname = os.path.join(fullname, file)
                displayname = file
                if os.path.isdir(child_fullname):
                    displayname = file + "/"
                res.append(f'<li><a href="{displayname}">{displayname}</a></li>')
            res.append(end_res)
            encoded = '\n'.join(res).encode('utf-8', 'surrogateescape')
            return HTMLResponse(encoded)
            # return {
            #     "msg": 200,
            #     "data": files
            # }
        # 如果是文件，直接下载
        return FileResponse(file_path)
    else:
        # return {
        #     "msg": 404,
        #     "data": "File not found"
        # }
        return HTMLResponse(error_res)


if __name__ == '__main__':
    uvicorn.run('http_server:app', host='127.0.0.1', port=8080, reload=True)
