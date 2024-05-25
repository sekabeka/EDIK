import requests
import os
import random
import time

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')


def proxys():
    time.sleep(random.randint(3, 6))
    response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
    data = response.json()
    result = []
    for item in data['list'].values():
        proxy = f'http://{item["user"]}:{item["pass"]}@{item["ip"]}:{item["port"]}'
        response = requests.get(
            'https://www.detmir.ru',
            proxies={
                'http' : proxy,
                'https' : proxy
            }
        )
        if response.ok:
            result.append(proxy)

    yield len(result)
    while True:
        for proxy in result:
            yield proxy


class ProxyRegister:
    proxies = proxys()
    length = next(proxies)
    bad_proxy = set()
    def process_request(self, request, spider):
        if self.length:
            spider.custom_settings['CONCURRENT_REQUESTS_PER_DOMAIN'] = self.length
            spider.custom_settings['CONCURRENT_REQUESTS'] = self.length
            self.length = None
        request.meta['proxy'] = next(self.proxies)
        return None

    def process_response(self, request, response, spider):
        if response.status == 418:
            print (request.meta['proxy'])
            return request
        return response