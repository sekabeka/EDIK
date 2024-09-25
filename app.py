import json
import schedule
import pandas as pd
import time

from scrapy.settings import Settings
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process

from src.spiders import detmirScraper, auchanScraper
from src.functions import get_list_proxy
from src.bot import send_result_table

def create_process(spider, proxylist):
    p = CrawlerProcess(Settings())
    p.crawl(spider, proxylist=proxylist)
    p.start()

def job():
    proxylist = get_list_proxy()
    ps = [
        Process(target=create_process, args=(spider, proxylist)) for spider in spiders
    ]
    for p in ps:
        p.start()

    for p in ps:
        p.join()

    result = []
    with open ('src/results/child.jsonl', 'r', encoding='utf-8') as file:
        result += [
            json.loads(s) for s in file.readlines()
        ]
    with open ('src/results/auchan.jsonl', 'r', encoding='utf-8') as file:
        result += [
            json.loads(s) for s in file.readlines()
        ]
    with pd.ExcelWriter('src/results/result.xlsx', mode='w', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls' : False}}) as writer:
        pd.DataFrame(result).to_excel(writer, index=False, sheet_name='all')
        for item in result:
            item['Параметр: Тип'] = None
        pd.DataFrame(result).to_excel(writer, index=False, sheet_name='clear_type')

    send_result_table(871881605)
    send_result_table(5107226763)
    
    
spiders = [
    detmirScraper,
    auchanScraper
]

#schedule.every().days.at("04:00", "Europe/Moscow").do(job)
if __name__ == '__main__':
    try:
        while True:
            #schedule.run_pending()
            #time.sleep(1)
            job()
            break
    except Exception as e:
        print (e)

    
    
    
    
    

