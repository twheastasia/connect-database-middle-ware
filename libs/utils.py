#-*- coding:utf-8 -*-
__author__ = 'twh'

import json
import yaml
import os
import datetime
import random
import copy
import string

# 读取json文件
def read_json(file):
    return json.load(open(file, 'r', encoding='utf-8'))

# 写入文件
def write_file(file_name, data):
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(data)

# 追加写入文件
def append_into_file(file_name, data):
    with open(file_name, 'a', encoding='utf-8') as file:
        file.write(data + '\n')

# 写入yaml文件
def write_yaml(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        yaml.dump(data=data, stream=f, allow_unicode=True)

# 读取yaml文件
def read_yaml(path):
    result = {}
    with open(path, "rb") as f:
        result = yaml.safe_load(f)
    return result

# 读取普通文件
def read_file(file):
    result = ''
    if os.path.exists(file):
        with open(file, 'r', encoding='utf-8') as f:
            result = f.read()
            f.close()
    return result

# 安全地创建目录
def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

# 巧妙的查找最大的文件名应该是哪个
def find_the_largest_name(dir, name):
    index = 0
    while True:
        index += 1
        formatted = name.format(index)
        path = os.path.join(dir, formatted)
        # filename didn't contain {index} or unique path was found
        if not os.path.exists(path):
            return path

# 创建随机字符串
def random_string(prefix=''):
    timestamp = datetime.datetime.now().timestamp()
    return '{0}{1}'.format(prefix, int(timestamp))

# 创建随机手机号
def random_number(length = 10):
    result = ''
    for index in range(length):
        result = '{0}{1}'.format(result, random.randrange(0, 9))
    return result

# 深度拷贝
def deep_copy(origin):
    return copy.deepcopy(origin)

# from chatgpt
# 随机英文名
def random_english_name(prefix=''):
    vowel = "aeiou"
    consonant = "".join(set(string.ascii_lowercase) - set(vowel))
    name = ""
    length = random.randint(5, 10)
    for i in range(length):
        if i % 2 == 0:
            name += random.choice(consonant)
        else:
            name += random.choice(vowel)
    return prefix + name.capitalize()

# 定义生成汉字函数
def create_chinese():
    head = random.randint(0xb0, 0xf7)
    body = random.randint(0xa1, 0xfe)
    val = f'{head:x}{body:x}'
    return bytes.fromhex(val).decode('gb18030')

# 随机中文姓名
def random_chinese_name(prefix=''):
    surname = create_chinese()  # 随机生成一个姓氏
    num = random.randint(1, 2)  # 随机生成名字的长度，1代表一个字，2代表两个字
    if num == 1:
        name = create_chinese()
    else:
        name = create_chinese() + create_chinese()
    return prefix + surname + name