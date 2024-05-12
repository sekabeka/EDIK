import requests

from config import API_KEY


def proxys():
    response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
    data = response.json()
    result = []
    for item in data['list'].values():
        result.append(f'http://{item["user"]}:{item["pass"]}@{item["ip"]}:{item["port"]}')
    while True:
        for proxy in result:
            yield proxy


class ProxyRegister:
    proxies = proxys()
   
    def process_request(self, request, spider):
        request.meta['proxy'] = next(self.proxies)
        return None

    def process_response(self, request, response, spider):
        return response