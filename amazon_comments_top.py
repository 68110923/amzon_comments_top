import sys, datetime
import django
import os, re
from openpyxl import Workbook

sys.path.extend(['D:\\project_django22_sui\\project'])
os.environ['DJANGO_SETTINGS_MODULE'] = 'project.settings'
django.setup()
from django.db.models import Sum, Avg, Max, Min, Count, F, Q
from django.db import transaction
from multiprocessing import Process
import multiprocessing
from crawler.amazon_crawler import Crawler
import time
import random
from it.models import CrlSelfCommentTop


def crawler_top_list(host):
    '''获取top列表'''
    self = Crawler()
    # for host in host_list:
    url = f'https://{host}/reviews/top-reviewers'
    self.get(url)
    while True:
        list_elements = self.driver.find_elements_by_xpath('//table[@role="tab"]/tbody/tr')[2:-2]
        for element in list_elements:
            data = {'site': host}
            data['rank'] = self.read(self.wait('xpath', 'td[1]', element=element, time_out=0))
            data['user_nick'] = self.read(self.wait('xpath', 'td[3]/a[1]', element=element, time_out=0)).strip()
            data['user_link_url'] = self.read(self.wait('xpath', 'td[3]/a[1]', element=element, time_out=0),attr='href', js=False)
            data['total_reviews'] = self.read(self.wait('xpath', 'td[4]', element=element, time_out=0),js=False).strip()
            data['helpful_votes'] = self.read(self.wait('xpath', 'td[5]', element=element, time_out=0))
            data['percent_helpful'] = self.read(self.wait('xpath', 'td[6]', element=element, time_out=0))
            for i in ['rank', 'total_reviews', 'helpful_votes', 'percent_helpful']:
                data[i] = self.plugin_str_to_int(data.get(i))
            data['user_link_url'] = re.sub('/ref=.*','',data['user_link_url'])
            CrlSelfCommentTop.objects.get_or_create(data,user_link_url=data['user_link_url'])
        next_page = self.wait('xpath', '//ul[@class="a-pagination"]//li[@class="a-last"]', time_out=0)
        if next_page:
            self.click(next_page, js=False)
            self.wait('xpath', '//table[@role="tab"]/tbody/tr')
        else:
            break
    self.driver.quit()


def crawler_tel_link():
    self = Crawler()
    self.driver.implicitly_wait(0.3)
    while True:
        save_id = transaction.savepoint()
        try:
            with transaction.atomic():
                # .exclude(site='www.amazon.de')
                temp = CrlSelfCommentTop.objects.filter(tel_link__isnull=True,status=1).first()
                if not temp:return
                user_link_url = temp.user_link_url
                CrlSelfCommentTop.objects.filter(user_link_url=user_link_url, status=1).update(status=2)
        except Exception as e:
            print(e)
            transaction.savepoint_rollback(save_id)
            continue
        else:
            transaction.savepoint_commit(save_id)
        try:
            self.get(user_link_url, sleep=random.uniform(1, 3))
        except:continue
        self.wait('xpath','//div[@class="a-size-base a-color-base read-more-text" or @class="a-section profile-at-content" or @class="a-section a-spacing-top-base bio-widget-footer" or @class="a-fixed-right-grid-col social-link" or @class="a-fixed-right-grid social-class a-spacing-base" or @class="a-section user-link-section"]|//span[@class="a-color-tertiary"]',time_out=10)
        self.wait('xpath','//div[@class="a-fixed-right-grid-col social-link" or @class="a-fixed-right-grid social-class a-spacing-base" or @class="a-section user-link-section"]//a',time_out=0.5)
        temp = self.driver.find_elements_by_xpath('//div[@class="a-fixed-right-grid-col social-link" or @class="a-fixed-right-grid social-class a-spacing-base" or @class="a-section user-link-section"]//a')
        tel_link = ','.join(set([self.read(i, attr='href') for i in temp]))
        with transaction.atomic():
            CrlSelfCommentTop.objects.filter(user_link_url=user_link_url,status=2).update(tel_link=tel_link,status=3)


def download_xlsx():
    q = ['站点', '排名', '评论者昵称', '评论者主页', '总评数', '投票数', '有用数', '联系方式(逗号隔开)']
    s = ['site', 'rank', 'user_nick', 'user_link_url', 'total_reviews', 'helpful_votes', 'percent_helpful', 'tel_link']
    qs = CrlSelfCommentTop.objects.filter(status=3).values_list(*s)
    # 创建一个workbook 设置编码
    file = Workbook()
    sheet = file.active
    sheet.append(q)
    for i in qs: sheet.append(i)
    file.save(f'评论top榜联系方式{datetime.datetime.now().strftime("%Y%m%d")}.xlsx')




if __name__ == '__main__':
    start_url = [
        # 'www.amazon.co.uk', # 英国无数据
        # 'www.amazon.ca',
        # 'www.amazon.es',
        # 'www.amazon.de',
        # 'www.amazon.fr',
        # 'www.amazon.com',
        # 'www.amazon.it',    #
        'www.amazon.co.jp',   # 日本
        # 'www.amazon.com.br'   # 西班牙
    ]
    # for host in start_url:
    #     process = Process(target=crawler_top_list, args=(host,))
    #     process.start()
    #     time.sleep(5)


    # for i in range(1):
    #     process = Process(target=crawler_tel_link, args=())
    #     process.start()
    #     time.sleep(15)

    # download_xlsx()

