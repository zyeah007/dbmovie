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
from pandas import DataFrame, Series
from pymongo import MongoClient
import numpy as np
from pylab import mpl
from datetime import datetime
import matplotlib.pyplot as plt

# 设置绘图中文显示
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


class commentsAnalysis(object):
    def __init__(self, database, movie_name):
        self.database = database
        self.movie_name = movie_name
        self.grades = {'力荐': 5, '推荐': 4, '还行': 3, '较差': 2, '很差': 1}

    def getData(self):
        '''
        从mongoDB中提取数据
        :return: 电影评论的DataFrame数据
        '''
        try:
            client = MongoClient()
            db = client[self.database]
            col = db[self.movie_name]
            df = DataFrame(list(col.find()))
            print('连接mongoDB成功！')
            return df
        except Exception:
            print('尚未启动mongoDB服务。请提前开启mongoDB服务！')

    def getCleanData(self):
        '''
        对从mongoDB中获取的原始数据进行清洗：剔除多余字段；转换数据类型；添加评分分数值列
        :return:
        '''
        df = self.getData()
        del df['_id']  # 删除mongodb自动生成对_id字段
        # 处理评分（rating）字段空字符串及缺失值
        df.rating = df.rating.str.split(',', expand=True).replace('', np.nan)
        grades = self.grades
        # 添加分数值列
        df['score'] = df['rating'].apply(lambda x: grades[x] if x in grades else 0)
        # 转换数据类型
        df['pub_time'] = pd.to_datetime(df['pub_time'])  # 将pub_time（评论发布日期）转换为日期格式
        df['vote'] = pd.to_numeric(df['vote'])  # 将vote(有用数）转换为数据格式
        print('共获取%d条评论信息.' % len(df))
        return df

    def ratingAnalysis(self, data):
        print('电影评分描述性统计分析结果如下：')
        print(data['score'].describe())  # 对评论分数做描述性统计分析
        counts = data['rating'].value_counts()
        counts.rename('counts', inplace=True)
        ratings = pd.concat([counts, Series(self.grades, name='score')], axis=1)
        ratings.dropna(inplace=True)
        ratings.sort_values(by='score', inplace=True)
        print('电影评论分数分布如下图：')
        ax = ratings['counts'].plot(title='电影评论分数分布图', kind='barh')
        fig = ax.get_figure()
        fig.savefig('电影评论分数分布图.png')

    def ratingByTime(self, data):
        '''
        分析样本中评分随时时间对变化，需要使用groupby技术
        :param data:
        :return:
        '''
        # 增加新的日期统计字段
        new_col = 'date_for_analysis'
        data[new_col] = data['pub_time'].apply(lambda x: datetime.strftime(x, '%m-%d'))
        rating_by_time = data['score'].groupby(by=data[new_col]).average()
        rating_by_time.sort_index()
        title = '电影评分时间变化图'
        print('%s:' % title)
        ax = rating_by_time.plot(title=title, kind='bar')
        fig = ax.get_figure()
        fig.savefig('%s.png' % title)

    def voteAnalysis(self, data):

        pass


# 评论发布时间分布情况

# TODO: 由于时间范围可能会很大，所以设置时间窗口，在该窗口内进行统计
# 评价分布
# TODO:根据评价得到对应的分数（满分5分），然后可以计算出平均分

if __name__ == '__main__':
    database = 'douban'
    movie_name = 'comments'
    dataAnalysis = commentsAnalysis(database, movie_name)
    data = dataAnalysis.getCleanData()
    dataAnalysis.ratingByTime(data)
