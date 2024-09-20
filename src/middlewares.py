import time
import random

from dataclasses import dataclass
from scrapy.crawler import Crawler
from playwright.sync_api import sync_playwright
from multiprocessing import Process, Queue

@dataclass
class SweetiePair:
    proxy_str: str
    proxy_dict: dict
    cookies: dict

def create_pw_instance(pair, q):
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        context = browser.new_context(proxy=pair.proxy_dict)
        while True:
            page = context.new_page()
            try:
                page.goto('https://www.auchan.ru', wait_until='load')
            except:
                pass
            page.wait_for_timeout(10 * 1000)
            cookies = {
                item['name'] : item['value'] for item in context.cookies()
            }
            page.close()
            context.clear_cookies()
            pair.cookies = cookies
            q.put(pair)
            time.sleep(random.randint(30, 120))

class AuchanMiddlewares:
    def __init__(self, proxy_list, spider):
        self.q = Queue(len(proxy_list))
        self.spider = spider
        self.spider.__setattr__('processes', [])
        for proxy in proxy_list:
            pair = SweetiePair(
                "http://{}:{}@{}".format(proxy['username'], proxy['password'], proxy['server']),
                proxy,
                {}
            )
            p = Process(target=create_pw_instance, args=(pair, self.q))
            p.start()
            self.spider.processes.append(p)
        self.pairs_g = self.generator()

    def generator(self):
        while 1:
            if self.q.full():
                lst = []
                while not self.q.empty():
                    lst.append(self.q.get())
                while not self.q.full():
                    for item in lst:
                        yield item        
         
    def process_request(self, request, spider):
        pair = next(self.pairs_g)
        request.cookies, request.meta['proxy'] = pair.cookies, pair.proxy_str
        return None

    def process_response(self, request, response, spider):
        if response.status == 401:
            return request
        return response
    
    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.spider.custom_settings
        proxy_list = crawler.spider.proxylist
        settings['CONCURRENT_REQUESTS_PER_DOMAIN'] = len(proxy_list)
        return cls(proxy_list, crawler.spider)

class DetmirMiddlewares:
    def __init__(self, proxylist=[]):
        self.proxy_g = self.generator_of_proxy(proxylist)

    def generator_of_proxy(self, proxy_list):
        while True:
            for proxy in proxy_list:
                yield "http://{}:{}@{}".format(
                    proxy['username'],
                    proxy['password'],
                    proxy['server']
                )

    def process_request(self, request, spider):
        request.meta['proxy'] = next(self.proxy_g)
        return None

    def process_response(self, request, response, spider):
        if response.status == 418:
            return request
        elif response.status == 403:
            return request
        return response
    
    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.spider.custom_settings
        proxy_list = crawler.spider.proxylist
        settings['CONCURRENT_REQUESTS_PER_DOMAIN'] = len(proxy_list)
        return cls(proxy_list)