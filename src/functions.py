import requests
import os

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')

def get_list_proxy():
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

def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        #del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v

def filter(object, key):
    return object.drop_duplicates(subset=[key])