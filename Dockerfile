# 
# FROM python:3.11.9
FROM twheastasia/connect-database-middleware-service-base:latest

# 
WORKDIR /code

# # 
# COPY ./requirements.txt /code/requirements.txt

# # 
# RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 
COPY . /code

# 
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]