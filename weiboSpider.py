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
from time import sleep, process_time
import requests
from selenium import webdriver
from lxml import etree
import signal
from multiprocessing import Process, Queue, Lock, Value, cpu_count
from tqdm import tqdm

# related file path
done_id_path = './ids.t'
undo_id_path = './undo_ids.t'
log_path = './log.t'

# some configure
options = webdriver.FirefoxOptions()
options.add_argument('--headless')
browser = webdriver.Firefox(options=options, executable_path='./geckodriver')
alive = Value('b', True)
save_path = os.path.abspath('.')
main_process_id = 0

processes = []
ids = set()
undo_ids = Queue()
lock = Lock()

duration = 0
start_time = process_time()
user_count = 0

log = {}

#######################################
#  your own configure
init_id = '2360812967'  # init_id is a seed id to start crawl (recommend a big user
crawl_all = 0  # 0 for only the original weibo created by user
all_user = 0  # 0 for normal user
since_date = '2019-08-01'  # crawl the weibo before the since_date


class Weibo(object):
    def __init__(self, user_id):
        self.weibo = []
        self.user = {}  # store user's information
        self.got_count = 0  # the number of weibo crawl
        self.user_id = user_id

    def start(self):
        """运行爬虫"""
        try:
            self.get_user_info()
            self.get_pages()

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

    def get_one_page(self, page):
        """获取一页的全部微博"""
        try:
            js = self.get_weibo_json(page)
            if js['ok']:
                weibos = js['data']['cards']
                for w in weibos:
                    if w['card_type'] == 9:
                        wb = get_one_weibo(w)
                        if wb:
                            created_at = datetime.strptime(wb['created_at'], "%Y-%m-%d")
                            since = datetime.strptime(since_date, "%Y-%m-%d")
                            if created_at < since:
                                if is_pin(w):
                                    continue
                                else:
                                    return True
                            if (not crawl_all) or ('retweet' not in wb.keys()):
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
            if not crawl_all:
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

    def get_filepath(self, t):
        """获取结果文件路径"""
        try:
            file_dir = save_path + os.sep + 'weibo' + os.sep + self.user['screen_name']

            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)

            file_path = file_dir + os.sep + self.user_id + '.' + t
            return file_path

        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def write_csv(self, wrote_count):
        """将爬到的信息写入csv文件"""
        write_info = self.get_write_info(wrote_count)
        result_headers = get_result_headers()
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

    def write_data(self, wrote_count):
        """将爬到的信息写入文件或数据库"""
        if self.got_count > wrote_count:
            self.write_csv(wrote_count)

    def get_pages(self):
        """获取全部微博"""
        page_count = self.get_page_count()

        # filter the zombie user

        # if page_count < 3:
        #     print(self.user['screen_name'] + '是一个假用户， 停止爬取....')
        #     print('*' * 100)
        #     return

        wrote_count = 0
        page1 = 0
        random_pages = random.randint(1, 5)
        # for test crawl only max 60 pages
        if page_count > 60:
            page_count = 60
        t = tqdm(range(1, page_count + 1), desc=self.user_id)
        for page in t:
            is_end = self.get_one_page(page)
            if is_end:
                t.update(t.total)
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

        self.write_data(wrote_count)  # write the rest weibo to file
        save_done_ids(self.user_id)


def get_long_weibo(weibo_id):
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
        weibo = parse_weibo(weibo_info)
        return weibo


def get_one_weibo(info):
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
                weibo = get_long_weibo(weibo_id)
                if not weibo:
                    weibo = parse_weibo(weibo_info)
            else:
                weibo = parse_weibo(weibo_info)
            if is_long_retweet:
                retweet = get_long_weibo(retweet_id)
                if not retweet:
                    retweet = parse_weibo(retweeted_status)
            else:
                retweet = parse_weibo(retweeted_status)
            retweet['created_at'] = standardize_date(
                retweeted_status['created_at'])
            weibo['retweet'] = retweet
        else:  # 原创
            if is_long:
                weibo = get_long_weibo(weibo_id)
                if not weibo:
                    weibo = parse_weibo(weibo_info)
            else:
                weibo = parse_weibo(weibo_info)
        weibo['created_at'] = standardize_date(
            weibo_info['created_at'])
        return weibo
    except Exception as e:
        print("Error: ", e)
        traceback.print_exc()


def get_result_headers():
    """获取要写入结果文件的表头"""
    result_headers = [
        'id', '正文', '原始图片url', '视频url', '位置', '日期', '工具', '点赞数', '评论数',
        '转发数', '话题', '@用户'
    ]
    if not crawl_all:
        result_headers2 = ['是否原创', '源用户id', '源用户昵称']
        result_headers3 = ['源微博' + r for r in result_headers]
        result_headers = result_headers + result_headers2 + result_headers3
    return result_headers


def parse_weibo(weibo_info):
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


def is_date(s_date):
    """判断日期格式是否正确"""
    try:
        datetime.strptime(s_date, "%Y-%m-%d")
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
    print('get log data ...')
    file = open(done_id_path, 'r')
    for line in file:
        if len(line.strip()) == 10:
            ids.add(line.strip())
    file.close()
    file1 = open(undo_id_path, 'r')
    for line in file1:
        if len(line.strip()) == 10:
            undo_ids.put(line.strip())
            ids.add(line.strip())
    file1.close()


def get_log():
    if os.path.exists(log_path):
        file = open(log_path, 'r')
        for line in file:
            tmp = line.strip().split(':')
            if len(tmp) == 2:
                log[tmp[0]] = tmp[1]
        file.close()
        if len(log.keys()) == 3:
            return

    log['number_of_run'] = '0'
    log['time_usage'] = '0'
    log['number_of_crawl_user'] = '0'


def save_log():
    t = process_time() - start_time
    log['time_usage'] = float(log['time_usage']) + t
    log['number_of_run'] = int(log['number_of_run']) + 1
    log['number_of_crawl_user'] = int(log['number_of_crawl_user']) + user_count
    file = open(log_path, 'w+')
    for key in log.keys():
        file.write(key + ":" + str(log[key]) + '\n')
    file.close()


def save_done_ids(user_id):
    lock.acquire()
    global user_count
    user_count += 1
    file = open(done_id_path, 'a+')
    file.write(user_id + '\n')
    file.close()
    lock.release()


def add_done_ids(user_id):
    lock.acquire()
    ids.add(user_id)
    lock.release()


def run(value):
    while value.value:
        user_id = undo_ids.get()
        get_related_ids(user_id)
        wb = Weibo(user_id)
        wb.start()


def exit_handle(signum, frame):
    # stop subprocess when the main process is canceled
    # and store the progress
    if os.getpid() == main_process_id:
        print('stopping (please wait) ... ')

        alive.value = False
        f = open(undo_id_path, 'w+')
        for i in range(undo_ids.qsize()):
            f.write(undo_ids.get() + '\n')
        f.close()
        print('save progress done.!')

        for p in processes:
            p.join()

        save_log()
        browser.quit()
        print('finished.!')


def get_related_ids(source_id):
    sleep_time = 10
    one = source_id
    count = 0
    browser.get('https://weibo.com/u/' + one + '?is_hot=1')
    sleep(sleep_time)
    html = browser.page_source
    regex = re.compile('/u/[0-9]{10}')
    if not isinstance(html, str):
        return True

    if undo_ids.qsize() < 50:
        # there are enough ids in Queue
        for each_id in tqdm(regex.findall(html), desc='Get [' + source_id + ']\'s ids'):
            tmp = each_id[3:]
            if tmp not in ids:
                add_done_ids(tmp)
                count += 1
                undo_ids.put(tmp)


# crawl single user
def crawl_single_user(user_id):
    wb = Weibo(user_id)
    wb.start()


def main():
    try:
        cpu = cpu_count()  # get cpu count of computer
        if cpu > 1:
            thread_num = cpu - 1
        else:
            thread_num = 1

        # for test only 4 thread allow
        thread_num = 4

        get_log()
        if log['number_of_run'] == '0':
            # first run
            add_done_ids(init_id)
            undo_ids.put(init_id)

        get_history_ids()
        for i in range(thread_num):
            p = Process(target=run, args=(alive,))
            p.start()
            processes.append(p)

        while alive.value:
            for p in processes:
                if not p.is_alive():
                    processes.remove(p)
                    print('subprocess dead!!!!!')
                    p = Process(target=run, args=(alive,))
                    p.start()
                    processes.append(p)
            sleep(10)

        for p in processes:
            p.terminate()

        print('quit')

    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()


signal.signal(signal.SIGTERM, exit_handle)
signal.signal(signal.SIGINT, exit_handle)
signal.signal(signal.SIGTSTP, exit_handle)

if __name__ == '__main__':
    main_process_id = os.getpid()
    main()
