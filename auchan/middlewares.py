import asyncio
import requests
import logging
import os
import time, random


from dotenv import load_dotenv
from playwright.async_api import async_playwright, BrowserContext

load_dotenv()

API_KEY = os.getenv('API_KEY')

class CookiesAuchan:
    def __init__(self):
        self.obj = TEST()
        self.result = [i for i in self.obj.run() if i is not None]
        self.length = len(self.result)
        self.set = True
        self.lst = self.MyGenerator()

    def MyGenerator(self):
        count = 0
        while True:
            for cookies, proxy in self.result:
                yield cookies, proxy
                count = count + 1
                if count == 1500:
                    self.result = [i for i in self.obj.run() if i is not None]
                    count = 0
                    break
        
    def process_request(self, request, spider):
        if self.set is not None:
            spider.custom_settings['COUCURRENT_REQUESTS']  = self.length + 2 
            spider.custom_settings['CONCURRENT_REQUEST_PER_DOMAIN']  = self.length + 1    
            self.set = None
        request.cookies, request.meta['proxy'] = next(self.lst)
        return None
    
    def process_response(self, request, response, spider):
        if response.status == 401:
            return request
        return response
    

class TEST:
    def __init__(self) -> None:
        self.result = self.proxys()

    async def _cookies(self, context:BrowserContext, proxy:dict):
        page = await context.new_page()
        try:
            await page.goto(
                url='https://www.auchan.ru',
                wait_until='load'
            )
        except:
            logging.error('Ошибка в получении cookies, пробуем еще раз')
            return None
        else:
            await page.wait_for_timeout(10 * 1000)

            cookies = await context.cookies()

            return cookies, f"http://{proxy['username']}:{proxy['password']}@{proxy['server']}"


    async def main(self):
        async with async_playwright() as p:
            browser = await p.firefox.launch()
            contexts = [
                (await browser.new_context(proxy=proxy), proxy) for proxy in self.result
            ]
            result = await asyncio.gather(
                *[self._cookies(*context) for context in contexts]
            )
        return result
            

    def run(self):
        logging.debug('Go update cookies :)')
        return asyncio.run(self.main())

    def proxys(self):
        time.sleep(random.randint(4, 6))
        response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")   
        data = response.json()
        result = []
        for item in data['list'].values():
            result.append({
                'server' : f'{item["ip"]}:{item["port"]}',
                'username' : item['user'],
                'password' : item['pass']
            })
        return result

        
