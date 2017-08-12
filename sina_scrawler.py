#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-07-29 15:00:35
# @Author  : jingray (lionel_jing@163.com)
# @Link    : http://www.jianshu.com/u/01fb0364467d
# @Version : $Id$

import os
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import pandas
import re
import sqlite3

CommentsURL = 'http://comment5.news.sina.com.cn/page/info?version=1&format=js&channel=gn&\
newsid=comos-{}&group=&compress=0&ie=utf-8&oe=utf-8&page=1&page_size=20'

url = 'http://api.roll.news.sina.com.cn/zt_list?channel=news&cat_1=gnxw&cat_2==gdxw1||=gatxw||=zs-pl||=mtjj&level==1||=2&show_ext=1&show_all=1&show_num=22&tag=1&format=json&page={}&callback=newsloadercallback&_=1501314053177'

def getCommentCounts(newsurl):
    m=re.search('doc-i(.*).shtml', newsurl)
    newsid = m.group(1)
    comments = requests.get(CommentsURL.format(newsid))
    jd = json.loads(comments.text.strip('var data='))
    return jd['result']['count']['total']

def getNewsDetail(newsurl):
    result = {}
    res = requests.get(newsurl)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result['title'] = soup.select('#artibodyTitle')[0].text
    result['newssource'] = soup.select('.time-source span a')[0].text
    timesource = soup.select('.time-source')[0].contents[0].strip()
    result['dt'] = datetime.strptime(timesource,'%Y年%m月%d日%H:%M')
    result['article'] = ' '.join([p.text.strip() for p in soup.select('#artibody p')[:-1]])
    result['editor'] = soup.select('.article-editor')[0].text.lstrip('责任编辑：')
    result['comments'] = getCommentCounts(newsurl)
    return result

def parseListLinks(url):
    newsdetails = []
    res = requests.get(url)
    jd = json.loads(res.text.lstrip('  newsloadercallback(').rstrip(');'))
    for ent in jd['result']['data']:
        newsdetails.append(getNewsDetail(ent['url']))
    return newsdetails


def main():
    news_total = []
    for i in range(1,3):
        newsurl = url.format(i)
        newsary = parseListLinks(newsurl)
        news_total.extend(newsary)

    df = pandas.DataFrame(news_total)

    df.to_excel('news.xlsx')

    with sqlite3.connect('news.sqlite') as db:
        df.to_sql('news',con = db)

    with sqlite3.connect('news.sqlite') as db:
        df2 = pandas.read_sql_query('SELECT * FROM news',con=db)
    print('scrawler sina news over!')

if __name__ == '__main__':
    main()
