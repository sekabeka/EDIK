import requests

from configs.config import API_KEY


def proxys():
    import time
    while True:
        response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
        if response.ok:
            break
        else:
            time.sleep(5)
    data = response.json()
    result = []
    for item in data['list'].values():
        if item["type"] == "http":
            result.append(f'http://{item["user"]}:{item["pass"]}@{item["ip"]}:{item["port"]}')
        else:
            print (item)
    yield len(result)
    while True:
        for proxy in result:
            yield proxy


class ProxyRegister:
    proxies = proxys()
    length = next(proxies)
    def process_request(self, request, spider):
        if self.length:
            spider.custom_settings['CONCURRENT_REQUESTS_PER_DOMAIN'] = self.length + 1
            spider.custom_settings['CONCURRENT_REQUESTS'] = self.length + 2
            self.length = None
        request.meta['proxy'] = next(self.proxies)
        return None

    def process_response(self, request, response, spider):
        if response.status == 418:
            return request
        return response
