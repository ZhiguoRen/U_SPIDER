# -*- coding: utf-8 -*-
import json
import time

def store_json_with_list(file_path,data_list,arg="w"):
    with open(file_path, arg) as json_file:
        for data in data_list:
            json_file.write(json.dumps(data))
            json_file.write("\n")



def store_json(file_path,data):
    with open(file_path, 'w') as json_file:
        json_file.write(json.dumps(data))

def load_json(file_path):
    try:
        with open(file_path,'r') as json_file: #w不存在则创建
            data = json.load(json_file)
            return data
    except Exception,e:
        print e
        with open(file_path, 'w') as json_file:
            print "create new file"
        return None


def timestamp_datetime(value, format = '%Y-%m-%d %H:%M:%S'):
    # value为传入的值为时间戳(整形)，如：1332888820
    value = time.localtime(value)
    ## 经过localtime转换后变成
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=0)
    # 最后再经过strftime函数转换为正常日期格式。
    dt = time.strftime(format, value)
    return dt

def datetime_timestamp(dt, format='%Y-%m-%d %H:%M:%S'):
    #dt为字符串
    #中间过程，一般都需要将字符串转化为时间数组
    time.strptime(dt, format)
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=-1)
    #将"2012-03-28 06:53:40"转化为时间戳
    s = time.mktime(time.strptime(dt, format))
    return int(s)

def datetime_timestamp_sticky(dt,format='%b %d, %Y %H:%M:%S'):
    #May 23, 2017
    time.strptime(dt,format)
    s=time.mktime(time.strptime(dt,format))
    return int(s)
