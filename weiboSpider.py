import codecs
import csv
import json
import math
import os
import random
import re
import sys
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from time import sleep

import requests
from selenium import webdriver
from lxml import etree
from tqdm import tqdm
from threading import Thread

done_id_path = './ids.t'

init_id = '2360812967'
get_all = 0
since_date = '2019-06-14'
mongodb_write = 0
mysql_write = 0
pic_download = 0
video_download = 0

options = webdriver.FirefoxOptions()
options.add_argument('--headless')
browser = webdriver.Firefox(options=options, executable_path='./geckodriver')

ids = []
undo_ids = []

save_path = os.path.abspath('.')


def is_date(since_date):
    """判断日期格式是否正确"""
    try:
        datetime.strptime(since_date, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def get_json(params):
    """获取网页中json数据"""
    url = 'https://m.weibo.cn/api/container/getIndex?'
    r = requests.get(url, params=params)
    return r.json()


def string_to_int(string):
    """字符串转换为整数"""
    if isinstance(string, int):
        return string
    elif string.endswith(u'万+'):
        string = int(string[:-2] + '0000')
    elif string.endswith(u'万'):
        string = int(string[:-1] + '0000')
    return int(string)


def standardize_date(created_at):
    """标准化微博发布时间"""
    if u"刚刚" in created_at:
        created_at = datetime.now().strftime("%Y-%m-%d")
    elif u"分钟" in created_at:
        minute = created_at[:created_at.find(u"分钟")]
        minute = timedelta(minutes=int(minute))
        created_at = (datetime.now() - minute).strftime("%Y-%m-%d")
    elif u"小时" in created_at:
        hour = created_at[:created_at.find(u"小时")]
        hour = timedelta(hours=int(hour))
        created_at = (datetime.now() - hour).strftime("%Y-%m-%d")
    elif u"昨天" in created_at:
        day = timedelta(days=1)
        created_at = (datetime.now() - day).strftime("%Y-%m-%d")
    elif created_at.count('-') == 1:
        year = datetime.now().strftime("%Y")
        created_at = year + "-" + created_at
    return created_at


def standardize_info(weibo):
    """标准化信息，去除乱码"""
    for k, v in weibo.items():
        if 'int' not in str(type(v)) and 'long' not in str(
                type(v)) and 'bool' not in str(type(v)):
            weibo[k] = v.replace(u"\u200b", "").encode(
                sys.stdout.encoding, "ignore").decode(sys.stdout.encoding)
    return weibo


def is_pin(info):
    """判断微博是否为置顶微博"""
    weibo_info = info['mblog']
    title = weibo_info.get('title')
    if title and title.get('text') == u'置顶':
        return True
    else:
        return False


def get_location(selector):
    """获取微博发布位置"""
    location_icon = 'timeline_card_small_location_default.png'
    span_list = selector.xpath('//span')
    location = ''
    for i, span in enumerate(span_list):
        if span.xpath('img/@src'):
            if location_icon in span.xpath('img/@src')[0]:
                location = span_list[i + 1].xpath('string(.)')
                break
    return location


def get_topics(selector):
    """获取参与的微博话题"""
    span_list = selector.xpath("//span[@class='surl-text']")
    topics = ''
    topic_list = []
    for span in span_list:
        text = span.xpath('string(.)')
        if len(text) > 2 and text[0] == '#' and text[-1] == '#':
            topic_list.append(text[1:-1])
    if topic_list:
        topics = ','.join(topic_list)
    return topics


def get_at_users(selector):
    """获取@用户"""
    a_list = selector.xpath('//a')
    at_users = ''
    at_list = []
    for a in a_list:
        if '@' + a.xpath('@href')[0][3:] == a.xpath('string(.)'):
            at_list.append(a.xpath('string(.)')[1:])
    if at_list:
        at_users = ','.join(at_list)
    return at_users


def get_user_list(file_name):
    """获取文件中的微博id信息"""
    with open(file_name, 'r') as f:
        user_id_list = f.read().splitlines()
    return user_id_list


def get_history_ids():
    file = open(done_id_path, 'r')
    for line in file:
        ids.append(line)
    file.close()


class Weibo(object):
    def __init__(self, user_id):
        self.weibo = []  # 存储爬取到的所有微博信息
        self.user = {}  # 存储目标微博用户信息
        self.got_count = 0  # 爬取到的微博数
        self.mysql_config = {}  # MySQL数据库连接配置，可以不填，当使用者的mysql用户名、密码等与本程序默认值不同时，需要通过mysql_config来自定义
        self.user_id = user_id

    def start(self):
        """运行爬虫"""
        try:
            # 将爬完的id加入ids中
            self.get_user_info()
            self.get_pages()
            # print(u'信息抓取完毕')
            # if self.pic_download == 1:
            #     self.download_files('img')
            # if self.video_download == 1:
            #     self.download_files('video')

        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_weibo_json(self, page):
        """获取网页中微博json数据"""
        params = {'containerid': '107603' + str(self.user_id), 'page': page}
        js = get_json(params)
        return js

    def get_user_info(self):
        """获取用户信息"""
        params = {'containerid': '100505' + str(self.user_id)}
        js = get_json(params)
        if js['ok']:
            info = js['data']['userInfo']
            if info.get('toolbar_menus'):
                del info['toolbar_menus']
            user_info = standardize_info(info)
            self.user = user_info
            return user_info

    def get_long_weibo(self, weibo_id):
        """获取长微博"""
        url = 'https://m.weibo.cn/detail/%s' % weibo_id
        html = requests.get(url).text
        html = html[html.find('"status":'):]
        html = html[:html.rfind('"hotScheme"')]
        html = html[:html.rfind(',')]
        html = '{' + html + '}'
        js = json.loads(html, strict=False)
        weibo_info = js.get('status')
        if weibo_info:
            weibo = self.parse_weibo(weibo_info)
            return weibo

    def parse_weibo(self, weibo_info):
        weibo = OrderedDict()
        if weibo_info['user']:
            weibo['user_id'] = weibo_info['user']['id']
            weibo['screen_name'] = weibo_info['user']['screen_name']
        else:
            weibo['user_id'] = ''
            weibo['screen_name'] = ''
        weibo['id'] = int(weibo_info['id'])
        text_body = weibo_info['text']
        selector = etree.HTML(text_body)
        weibo['text'] = etree.HTML(text_body).xpath('string(.)')
        weibo['location'] = get_location(selector)
        weibo['created_at'] = weibo_info['created_at']
        weibo['source'] = weibo_info['source']
        weibo['attitudes_count'] = string_to_int(
            weibo_info['attitudes_count'])
        weibo['comments_count'] = string_to_int(
            weibo_info['comments_count'])
        weibo['reposts_count'] = string_to_int(
            weibo_info['reposts_count'])
        weibo['topics'] = get_topics(selector)
        weibo['at_users'] = get_at_users(selector)
        return standardize_info(weibo)

    def get_one_weibo(self, info):
        """获取一条微博的全部信息"""
        try:
            weibo_info = info['mblog']
            weibo_id = weibo_info['id']
            retweeted_status = weibo_info.get('retweeted_status')
            is_long = weibo_info['isLongText']
            if retweeted_status:  # 转发
                retweet_id = retweeted_status['id']
                is_long_retweet = retweeted_status['isLongText']
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
                if is_long_retweet:
                    retweet = self.get_long_weibo(retweet_id)
                    if not retweet:
                        retweet = self.parse_weibo(retweeted_status)
                else:
                    retweet = self.parse_weibo(retweeted_status)
                retweet['created_at'] = standardize_date(
                    retweeted_status['created_at'])
                weibo['retweet'] = retweet
            else:  # 原创
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
            weibo['created_at'] = standardize_date(
                weibo_info['created_at'])
            return weibo
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()

    def get_one_page(self, page):
        """获取一页的全部微博"""
        try:
            js = self.get_weibo_json(page)
            if js['ok']:
                weibos = js['data']['cards']
                for w in weibos:
                    if w['card_type'] == 9:
                        wb = self.get_one_weibo(w)
                        if wb:
                            created_at = datetime.strptime(wb['created_at'], "%Y-%m-%d")
                            since = datetime.strptime(since_date, "%Y-%m-%d")
                            if created_at < since:
                                if is_pin(w):
                                    continue
                                else:
                                    return True
                            if (not get_all) or ('retweet' not in wb.keys()):
                                self.weibo.append(wb)
                                self.got_count = self.got_count + 1
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()

    def get_page_count(self):
        """获取微博页数"""
        weibo_count = self.user['statuses_count']
        page_count = int(math.ceil(weibo_count / 10.0))
        return page_count

    def get_write_info(self, wrote_count):
        """获取要写入的微博信息"""
        write_info = []
        for w in self.weibo[wrote_count:]:
            wb = OrderedDict()
            for k, v in w.items():
                if k not in ['user_id', 'screen_name', 'retweet']:
                    if 'unicode' in str(type(v)):
                        v = v.encode('utf-8')
                    wb[k] = v
            if not get_all:
                if w.get('retweet'):
                    wb['is_original'] = False
                    for k2, v2 in w['retweet'].items():
                        if 'unicode' in str(type(v2)):
                            v2 = v2.encode('utf-8')
                        wb['retweet_' + k2] = v2
                else:
                    wb['is_original'] = True
            write_info.append(wb)
        return write_info

    def get_filepath(self, type):
        """获取结果文件路径"""
        try:
            file_dir = save_path + os.sep + 'weibo' + os.sep + self.user['screen_name']

            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            if type == 'img' or type == 'video':
                file_dir = file_dir + os.sep + type
            if type == 'img' or type == 'video':
                return file_dir
            file_path = file_dir + os.sep + self.user_id + '.' + type
            return file_path

        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_result_headers(self):
        """获取要写入结果文件的表头"""
        result_headers = [
            'id', '正文', '原始图片url', '视频url', '位置', '日期', '工具', '点赞数', '评论数',
            '转发数', '话题', '@用户'
        ]
        if not get_all:
            result_headers2 = ['是否原创', '源用户id', '源用户昵称']
            result_headers3 = ['源微博' + r for r in result_headers]
            result_headers = result_headers + result_headers2 + result_headers3
        return result_headers

    def write_csv(self, wrote_count):
        """将爬到的信息写入csv文件"""
        write_info = self.get_write_info(wrote_count)
        result_headers = self.get_result_headers()
        result_data = [w.values() for w in write_info]
        if sys.version < '3':  # python2.x
            with open(self.get_filepath('csv'), 'ab') as f:
                f.write(codecs.BOM_UTF8)
                writer = csv.writer(f)
                if wrote_count == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
        else:  # python3.x
            with open(self.get_filepath('csv'),
                      'a',
                      encoding='utf-8-sig',
                      newline='') as f:
                writer = csv.writer(f)
                if wrote_count == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
        # print(u'%d条微博写入csv文件完毕,保存路径:' % self.got_count)
        # print(self.get_filepath('csv'))

    def write_data(self, wrote_count):
        """将爬到的信息写入文件或数据库"""
        if self.got_count > wrote_count:
            self.write_csv(wrote_count)
            # if mysql_write:
            #     self.write_mysql(wrote_count)
            # if mongodb_write:
            #     self.write_mongodb(wrote_count)

    def get_pages(self):
        """获取全部微博"""
        page_count = self.get_page_count()
        print('*' * 100)
        print("开始爬取 " + self.user['screen_name'] + '[' + self.user_id + ']' + "　的微博")
        # 如果总微博数大于50则是合法用户
        if page_count < 3:
            print(self.user['screen_name'] + '是一个假用户， 停止爬取....')
            print('*' * 100)
            return
        wrote_count = 0
        page1 = 0
        random_pages = random.randint(1, 5)
        for page in range(1, page_count + 1):
            # print(u'第%d页' % page)
            is_end = self.get_one_page(page)
            if is_end:
                break

            if page % 20 == 0:  # 每爬20页写入一次文件
                self.write_data(wrote_count)
                wrote_count = self.got_count

            # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
            # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
            # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
            if page - page1 == random_pages and page < page_count:
                sleep(random.randint(6, 10))
                page1 = page
                random_pages = random.randint(1, 5)

        self.write_data(wrote_count)  # 将剩余不足20页的微博写入文件
        print(u'微博爬取完成，共爬取%d条微博' % self.got_count)
        print('*' * 100)


def run(user_id):
    '''多线程爬取'''
    wb = Weibo(user_id)
    wb.start()


def get_related_ids(source_id=[]):
    '''获取每个微博页面中的用户id用于爬取'''
    if len(undo_ids) > 20:
        # 带爬取的ids列表还有大量用户
        return
    for one in source_id:
        print('-' * 100)
        print("开始获取　" + one + " 的相关id")
        count = 0
        browser.get('https://weibo.com/u/' + one + '?is_hot=1')
        sleep(10)
        html = browser.page_source
        regex = re.compile('/u/[0-9]{10}')
        for each_id in regex.findall(html):
            tmp = each_id[3:]
            if tmp not in ids and tmp not in undo_ids:
                # print("from  " + one + " get " + tmp)
                count += 1
                undo_ids.append(tmp)
        print("共获取 " + str(count))
        print('-' * 100)


def main():
    try:
        thread_num = 5
        get_history_ids()  # 获取已经爬取过的用户id
        get_related_ids(source_id=[init_id])
        while True:
            threads = []
            temp_ids = []
            for i in range(thread_num):
                undo_id = undo_ids.pop(0)
                ids.append(undo_id)
                if not undo_id:
                    print("finished！！！！")
                    return
                t = Thread(target=run, args=(undo_id,))
                t.start()
                temp_ids.append(undo_id)
                threads.append(t)

            th = Thread(target=get_related_ids, args=(temp_ids,))
            th.start()

            for t in threads:
                t.join()
            th.join()

    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()


if __name__ == '__main__':
    undo_ids.append(init_id)
    main()
