from scrapy.settings import Settings
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process

from src.spiders import detmirScraper, auchanScraper
from src.functions import get_list_proxy

def create_process(spider, proxylist):
    p = CrawlerProcess(Settings())
    p.crawl(spider, proxylist=proxylist)
    p.start()

    
if __name__ == '__main__':
    proxylist = get_list_proxy()
    spiders = [
        detmirScraper,
        auchanScraper
    ]
    ps = [
        Process(target=create_process, args=(spider, proxylist)) for spider in spiders
    ]

    for p in ps:
        p.start()

    for p in ps:
        p.join()


