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
from pandas import DataFrame, Series
from pymongo import MongoClient
import numpy as np
from pylab import mpl
from datetime import datetime
import matplotlib.pyplot as plt
import jieba
import pandas as pd
from wordcloud import WordCloud

# 设置绘图中文显示
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False


class commentsAnalysis(object):
    def __init__(self, database, movie_name):
        self.database = database
        self.movie_name = movie_name
        self.grades = {'力荐': 5, '推荐': 4, '还行': 3, '较差': 2, '很差': 1}
        self.stopwords = pd.read_excel('./stopwords.xlsx')
        self.userdict = open('./newdict.txt')
        self.font_path = '/Library/Fonts/simhei.ttf'

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
        ratings['prop'] = ratings['counts'] / ratings['counts'].sum()
        fig, ax = plt.subplots()
        y_ticks = ratings['score'].values
        y_labels = np.array(['1星', '2星', '3星', '4星', '5星'])
        print('电影评论分数分布如下图：')
        ax.barh(y_ticks, ratings['prop'], color='orange', alpha=1)
        ax.set_yticks(y_ticks)
        ax.set_xticks([])
        ax.set_yticklabels(y_labels)
        for a, b in zip(ratings.prop.values, y_ticks):  # 在图中添加数据标签
            ax.text(a + 0.01, b, '%.2f%%' % (a * 100))
        ax.set_xlim(0, 0.4)
        ax.text(0, 6, '豆瓣评分 %.1f' % (2 * data.score.mean()))
        fig.savefig('电影评论分数分布图.png')

    def ratingByTime(self, data):
        '''
        分析样本中评分随时时间的变化，需要使用groupby技术
        :param data:
        :return:
        '''
        # 增加新的日期统计字段
        new_col = 'month'
        data[new_col] = data['pub_time'].apply(lambda x: datetime.strftime(x, '%Y-%m'))
        rating_by_time = data['score'].groupby(by=data[new_col]).mean() * 2  # 原始数据是5分制，这里改成10分制
        rating_by_time.sort_index()
        title = '电影评分时间变化图'
        print('%s:' % title)
        fig, ax = plt.subplots()
        ax.plot(rating_by_time, linestyle='--', marker='o')
        ax.axes.set_xticklabels(rating_by_time.index.values, rotation=45)
        ax.set_title(title)
        fig.savefig('%s.png' % title)

    def getWordFreq(self, text_arr):
        '''
        获取词频
        :param text_arr:待分析的文本数组
        :return: 经过分词后的词组及词频数
        '''
        segment = self.sentence_seg(text_arr)
        word_count = segment.value_counts()
        return word_count

    def sentence_seg(self, text_arr):
        '''
        利用jieba库进行分词，并去掉停用词，返回dataframe数据
        :param text: 待分词文本数组
        :return:去掉停用词后分词表，dataframe结构
        '''
        jieba.load_userdict(self.userdict)
        result = pd.DataFrame()
        try:
            for t in text_arr:
                seg = DataFrame(jieba.cut(t), columns=['word'])  # 对每条评论进行分词
                seg['word'] = seg['word'].str.strip()  # 去掉\n符号
                seg['word_len'] = seg['word'].str.len()
                seg = seg[seg['word_len'] > 1]
                seg = seg[~seg['word'].isin([''])]  # 去掉空字符的行
                seg = seg[~seg['word'].isin(self.stopwords.stopword)]  # 去掉停用词
                result = pd.concat([seg, result], ignore_index=True)
            segment = result['word']
            return segment
        except Exception:
            print('分词失败！')
            return None

    def drawWordCloud(self, word_count, width=800, height=400, background_color='white', background_img=None,
                      max_words=200, max_font_size=80):

        wc = WordCloud(font_path=self.font_path, width=width, height=height, max_words=max_words,
                       max_font_size=max_font_size,
                       background_color=background_color, mask=background_img)
        my_wordcloud = wc.generate_from_frequencies(word_count, max_font_size=max_font_size)
        return my_wordcloud


'''
if __name__ == '__main__':
    database = input('请输入要连接的数据库名称：')
    movie_name = input('请输入集合名称：')
    dataAnalysis = commentsAnalysis(database, movie_name)
    data = dataAnalysis.getCleanData()
    word_count = dataAnalysis.getWordFreq(data['comment_lines'])
    my_wordcloud = dataAnalysis.drawWordCloud(word_count)
    my_wordcloud.to_file('img.png')
'''
