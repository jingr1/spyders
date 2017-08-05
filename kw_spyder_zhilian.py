#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-07-22 10:54:24
# @Author  : jingray (lionel_jing@163.com)
# @Link    : http://www.jianshu.com/u/01fb0364467d
# @Version : V1.0

#ERROR: UnicodeDecodeError: 'ascii' codec can't decode byte 0xe4 in position 0:
# ordinal not in range(128)
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
#ERROR END

import os

from datetime import datetime
from urllib import urlencode
from multiprocessing import Pool
import requests
from bs4 import BeautifulSoup
import pymongo
from kw_config_zhilian import *
import time
from itertools import product
import pandas

client = pymongo.MongoClient(MONGO_URL)
db = client[MONGO_DB]

def download(url):
    headers = {'User-Agent':'Mozilla/5.0(Windows NT 6.1;WOW64;rv:51.0) \
    Gecko/20100101 Firefox/51.0'}
    respons = requests.get(url, headers = headers)
    return respons.text

def get_content(html):
    #record the date
    date = datetime.now().date()
    date = datetime.strftime(date,'%Y-%m-%d') #convert to str

    soup = BeautifulSoup(html,'lxml')
    body = soup.body
    data_main = body.find('div',{'class':'newlist_list_content'})
    if data_main:
        tables = data_main.find_all('table')

        for i, table_info in enumerate(tables):
            if i == 0:
                continue
            tds = table_info.find('tr').find_all('td')
            job_title = tds[0].find('a').get_text() #job title
            job_link = tds[0].find('a').get('href') #job link
            feedback_rate = tds[1].find('span').get_text()
            corporate_name = tds[2].find('a').get_text()
            salary = tds[3].get_text()
            workplace = tds[4].get_text()
            release_date = tds[5].find('span').get_text()

            tr_brief = table_info.find('tr',{'class':'newlist_tr_detail'})

            #job profile
            breif = tr_brief.find('li',{'class':'newlist_deatil_last'}).get_text()

            #get information by generater
            yield{ 'job_title':job_title,
                   'feedback_rate':feedback_rate,
                   'job_link':job_link,
                   'corporate_name':corporate_name,
                   'salary':salary,
                   'workplace':workplace,
                   'release_date':release_date,
                   'breif':breif,
                   'savedate':date
                 }

def main(args):
    basic_url ='http://sou.zhaopin.com/jobs/searchresult.ashx?'
    itemdata = []
    for keyword in KEYWORDS:
        mongo_table = db[keyword]
        paras = {'jl':args[0],
                 'kw':keyword,
                 'p' :args[1]
                 }
        url = basic_url + urlencode(paras)
        #print(url)
        html = download(url)
        #print(html)
        if html:
            data = get_content(html)
            for item in data:
                if mongo_table.update({'job_link':item['job_link']},{'$set':item},True): # avoid the repetitive item
                    #print 'saved records:'
                    #print item['job_title'].encode('utf-8')+','+item['job_link']+','+item['salary']
                    itemdata.append(item)
    return itemdata


if __name__ == '__main__':
    start = time.time()
    savedfile = open(SAVEDFILE,'w')
    dictlist =[]
    number_list = list(range(TOTAL_PAGE_NUMBER))
    args = product(ADDRESS,number_list)
    pool = Pool()
    resultdata = pool.map(main,args) #multiprocess
    for index in range(len(resultdata)):
        for dictindex in range(len(resultdata[index])):
            dictlist.append(resultdata[index][dictindex])
    df = pandas.DataFrame(dictlist)
    df.to_excel('zhilian.xlsx')
    for dictindex in range(len(dictlist)):
        filelist =[]
        for key in dictlist[dictindex]:
            filelist.append(dictlist[dictindex][key])
            filelist.append('\t')
        filelist.append('\n')
        savedfile.writelines(filelist)
    savedfile.close()
    end = time.time()
    print 'Finished,task runs %s seconds.' %(end-start)


