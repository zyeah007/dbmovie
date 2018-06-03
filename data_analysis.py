#!/usr/bin/env python
# coding=utf-8
'''
从mongoDB数据库中导出已经爬取的电影评论，并试着做如下分析：
（1）"有用"投票数的分布情况
（2）得到点赞前10的用户昵称及点赞数
（3）评分分布情况，平均分
（4）评论发布时间与评论数量之间的关系
（5）对全部评论内容进行文本分析
'''

import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient
import numpy as np

client = MongoClient()
db = client['douban']
col = db['your_name']
allData = DataFrame(list(col.find()))
# 删除mongoDB中自带对_id字段
del allData['_id']
# 判断哪些列存在缺失值
allData.isnull().any()
# 处理缺失值或空字符串
allData.rating=allData.iloc[:,2].str.split(',',expand=True).replace('',np.nan)
# 评论发布时间分布情况

allData['pub_time'] = pd.to_datetime(allData['pub_time'])  # 将pub_time列转换为日期格式
print(allData['pub_time'].value_counts())
# TODO: 由于时间范围可能会很大，所以设置时间窗口，在该窗口内进行统计
# 评价分布
print(allData['rating'].value_counts)
# TODO:根据评价得到对应的分数（满分5分），然后可以计算出平均分
allData[allData['rating'] == ''] = '无'  # 将没有打分的评论评分设置为"无"
grades = {'力荐': 5, '推荐': 4, '还行': 3, '较差': 2, '很差': 1, '无': 0}
allData['score'] = allData['rating'].apply(lambda x: grades[x])
