#!/usr/bin/env python
# coding=utf-8
'''
从豆瓣电影抓取影片《超时空同居》的短评；将数据用mongo数据库存储；用pandas对电影评论数据进行分析
'''
import requests
from lxml import etree
import re
from pymongo import MongoClient
import random
import time


class dbSpider(object):
    def __init__(self, db, collection, cookies):
        self.db = db
        self.collection = collection
        self.cookies = cookies

    def get_html(self, url):
        try:
            r = requests.get(url, timeout=5, cookies={'cookie': self.cookies})
            r.raise_for_status()
            html = etree.HTML(r.text)
            return html
        except Exception:
            print('解析页面失败！')
            time.sleep(2)
            self.get_html(url)

    def next_page_url(self, cur_url):
        '''
        从当前页面解析下一页的url
        :param html: 当前页面当html文件
        :return: 下一页的url链接
        '''
        html = self.get_html(cur_url)
        base_url = re.search(r'^https:.*comments', cur_url).group(0)
        paginator = html.xpath('//div[@id="paginator"]//a')
        for a in paginator:
            match = re.search(r'[\u4E00-\u9FA5]+', a.text)
            if match.group(0) == '后页':
                next_url = base_url + a.attrib['href']
                return next_url
        else:
            print('已经是最后一页！')
            return None

    def get_comments(self, cur_url):
        '''
        从当前html页面解析出20条评论的信息,并存入mongo数据库。获取每条评论的id,日期，评论内容，有用数量
        :param cur_url:
        :return:当前页面的20条评论数据信息
        '''
        print('解析页面:%s' % cur_url)
        html = self.get_html(cur_url)
        commList = html.xpath('//div[@class="comment-item"]')
        data = []
        for item in commList:
            user_id = item.attrib['data-cid']
            vote = item.xpath('.//span[@class="votes"]')[0].text.strip()
            user_name = item.xpath('.//span[@class="comment-info"]/a')[0].text.strip()
            status = item.xpath('.//span[@class="comment-info"]//span[1]/text()')[0].strip()
            if len(item.xpath('.//span[@class="comment-info"]//span')) == 3:
                rating = item.xpath('.//span[@class="comment-info"]//span[2]/@title')[0].strip()
                pub_time = item.xpath('.//span[@class="comment-info"]//span[3]/text()')[0].strip()
            else:
                rating = ''
                pub_time = item.xpath('.//span[@class="comment-info"]//span[2]/text()')[0].strip()
            comment_lines = item.xpath('.//p/text()')[0].strip()
            comment_info = {
                'user_id': user_id,
                'vote': vote,
                'user_name': user_name,
                'status': status,
                'rating': rating,
                'pub_time': pub_time,
                'comment_lines': comment_lines
            }
            data.append(comment_info)
        return data

    def saveData(self, data):
        '''

        :param data:
        :param db:
        :return:
        '''
        client = MongoClient()
        mongo_DB = self.db
        db = client[mongo_DB]
        col = db[self.collection]
        try:
            if col.insert_many(data):
                print('保存成功！')
        except Exception:
            print('保存失败。')
            return None

    def dbCrawl(self, cur_url, pageNum):
        '''
        爬取指定页数的评论，然后停止
        :param start_url:起始页
        :param pageNum: 指定需要爬取的评论页面数
        :return:
        '''
        i = 1
        while cur_url:
            if i > pageNum:
                break
            else:
                data = self.get_comments(cur_url)
                self.saveData(data)
                print('成功爬取第%d页！' % i)
                i += 1
                cur_url = self.next_page_url(cur_url)
                time.sleep(2)
        print('爬取结束！')


def get_cookies(raw_lines):
    '''
    用cookies登陆网站，才能进行更多的评论浏览。将原始cookies字符串raw_cookies转换成字段格式
    add line for test
    :param raw_lines: 原始cookies字符串。需每次从网站复制
    :return: 字典格式的cookies
    '''
    cookies = {}
    for line in raw_lines.split(';'):
        key, value = line.split('=', 1)
        cookies[key] = value
    return cookies


if __name__ == '__main__':
    client = MongoClient()
    db = 'douban'
    collection = 'comments'
    raw_cookies = 'll="108288"; bid=2uMpC4PcO5k; __utmz=30149280.1513535714.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _vwo_uuid_v2=F403C9A2B9A079E30D8EB069525BF65A|79d71d8a8a48b0d26c96f8d9355ba2cf; _ga=GA1.2.1604958148.1513535714; __yadk_uid=Zk3qdOYuIbxnhssM4kpUtguyMkmOfRgO; ps=y; ue="zyquark@163.com"; push_doumail_num=0; ap=1; __utmv=30149280.4850; ct=y; __utmc=30149280; dbcl2="48504863:w9MJkEjFIgI"; _gid=GA1.2.1754008071.1527690786; ck=ETI6; push_noty_num=0; _pk_ref.100001.8cb4=%5B%22%22%2C%22%22%2C1527720445%2C%22https%3A%2F%2Fmovie.douban.com%2Fsubject%2F27133303%2Fcomments%3Fstart%3D240%26limit%3D20%26sort%3Dnew_score%26status%3DP%26percent_type%3D%22%5D; __utma=30149280.1604958148.1513535714.1527690786.1527720446.10; _pk_id.100001.8cb4=ac1edbf554daefc8.1513535713.8.1527720663.1527691223.'
    cookies = {'cookie': raw_cookies}
    spider = dbSpider(db=db, collection=collection, cookies=raw_cookies)
    print('开始爬取......')
    start_url = 'https://movie.douban.com/subject/27133303/comments?status=P'
    spider.dbCrawl(cur_url=start_url, pageNum=20) #只能爬取500条评论
    print('共爬取%d条评论' % client[db][collection].count())
    print('下一步进入分析')
