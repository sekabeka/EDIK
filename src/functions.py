import requests
import os
import pandas as pd

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


def get_input_table_values(name):
    p = pd.read_excel('src/input_table.xlsx', sheet_name='Лист2')
    p = p.fillna(0)
    p = p.to_dict('list')
    for market, root, sub1, sub2, url, breadcrumbs, formula, special_contidion, special_formula in zip(
        p.get('Магазин'),
        p.get('Корневая'),
        p.get('Подкатегория 1'),
        p.get('Подкатегория 2'),
        p.get('Ссылка на категорию товаров'),
        p.get('Размещение на сайте'),
        p.get('Формула расчета'),
        p.get('Особые условия'),
        p.get('Особые формулы расчета'),
    ): 
        if name != market.casefold():
            continue
        sub1 = sub1 if sub1 else None
        sub2 = sub2 if sub2 else None
        formula = formula.split('=')[-1]
        constraints = {}
        if special_contidion and special_formula:
            for condition, _formula in zip(special_contidion.split(';'), special_formula.split(';')):
                constraints[condition.casefold()] = _formula.split('=')[-1]
        yield (
            root, 
            sub1,
            sub2,
            url,
            breadcrumbs,
            formula,
            constraints
        )
