import lxml
import re
import scrapy
import scrapy.core.engine
import scrapy.http
import pandas as pd
import json
import requests
import os
import random
import time


from scrapy.settings import Settings
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('API_KEY')



def proxys():
    time.sleep(random.randint(2, 5))
    response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
    data = response.json()
    result = []
    for item in data['list'].values():
        result.append(f'http://{item["user"]}:{item["pass"]}@{item["ip"]}:{item["port"]}')
    yield len(result)
    while True:
        for proxy in result:
            yield proxy


class ProxyRegister:
    def process_request(self, request, spider):
        request.meta['proxy'] = next(spider.proxies)
        return None

    def process_response(self, request, response, spider):
        return response


def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v

class RED(scrapy.Spider):
    name = 'red'
    proxies = proxys()
    length = next(proxies)
    custom_settings = {
        "FEEDS" : {
            'red/red.jsonl' : {
                'format' : 'jsonlines',
                'overwrite' : True,
                #'indent' : 4,
                #'ensure_ascii' : False,
                'encoding' : 'utf-8'
            }
        },
        "LOG_FILE" : 'red/red.log',
        "LOG_FILE_APPEND" : False,
        "DOWNLOADER_MIDDLEWARES" : {
            'red.result.ProxyRegister' : 543
        },
        'CONCURRENT_REQUESTS' : length * 2,
        'CONCURRENT_REQUESTS_PER_DOMAIN' : length * 2
    }

    def start_requests(self):
        p = pd.read_excel("generalTable.xlsx", sheet_name='KARM').to_dict('list')
        start_urls = p['Ссылки на категории товаров']
        placements = p['Размещение на сайте']
        deliveries = p['Параметр: Доставка']
        prefixs = p['Префиксы']
        root_category = p['Корневая']
        first_category = p['Подкатегория 1']
        second_category = p['Подкатегория 2']
        formulas = p['Формула цены продажи']
        margins = p['Маржа']
        exceptions = [i.casefold().strip() for i in list(filter(lambda x: type(x) == str, p['Исключения']))]
        idx = 1
        for url, placement, delivery, prefix, root, cat1, cat2, fm, mg in zip(start_urls, placements, deliveries, prefixs, root_category, first_category, second_category, formulas, margins):
            kwargs = {
                'Корневая' : root,
                'Подкатегория 1' : cat1,
                'Подкатегория 2' : cat2,
                'Ссылка на категорию товаров' : url,
                'Расположение на сайте' : placement,
                'Параметр: Доставка' : delivery,
                'Префикс' : prefix,
                'Номер' : idx,
                'Формула' : fm.split('=')[-1],
                'Маржа' : mg,
                'Исключения' : exceptions
            }
            idx = idx + 1
            yield scrapy.Request(
                url = url,
                cb_kwargs = kwargs,
                callback = self.catalog,
                dont_filter=True
            )

    def Table(self, table, about):
        keys = [re.sub('\W', '', i.text) for i in table.thead.find_all('th')[:-2]]
        products = table.tbody.find_all('tr')
        result = []
        if 'Название' in keys:
            del about['Название товара или услуги']
        for product in products:
            tmp = {}
            for prop, key in zip(product.find_all('td'), keys):
                match key.casefold():
                    case "вариация":
                        tmp["Свойство: Вариант"] = "https://krasniykarandash.ru" + prop.a['href']
                    case 'цена':
                        tmp["Цена закупки"] = re.sub(r'\D', '', prop.find(class_='price-current').text)
                        if prop.find(class_='price-old') is not None:
                            tmp['Старая цена']  = re.sub(r'\D', '', prop.find(class_='price-old').text)
                    case _ :
                        if key == 'Артикул':
                            tmp[key] = prop.text.strip()
                        else:
                            tmp["Параметр: {}".format(key)] = prop.text.strip()
            result.append({**tmp, **about})
        return result
            
    def product(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        soup_markers = soup.find('div', class_='prod__labels')
        if soup_markers is not None:
            markers = ' '.join([i.text for i in soup_markers.find_all('div')])
        else:
            markers = None
        soup_decsr = soup.find(id="contentDescription")
        if soup_decsr is not None:
            description = soup_decsr.text
        else:
            description = None
        images = ' '.join(["https://krasniykarandash.ru" + i['href'] for i in soup.find_all('a', attrs={'data-fancybox' : 'images'})])

        prefix = kwargs.pop('Префикс')
        margin = kwargs.pop('Маржа')
        formulae = kwargs.pop('Формула')
        result = {
            'Параметр: Поставщик' : "KARM",
            "Параметр: Доставка" :  kwargs.pop('Параметр: Доставка'),
            'Свойство: Вариант' : None,
            "Вес, кг" : None,
            'Корневая' : kwargs.pop('Корневая'),
            'Подкатегория 1' : kwargs.pop('Подкатегория 1'),
            "Подкатегория 2" : kwargs.pop('Подкатегория 2'),
            'Название товара или услуги' : None,
            'Полное описание' : description,
            'Краткое описание' : None,
            'Параметр: Бренд' : None,
            'Размещение на сайте' : kwargs.pop('Расположение на сайте'),
            'Артикул' : None,
            "Артикул поставщика" : None,
            'Цена продажи' : None,
            'Старая цена' : None,
            'Цена закупки' : None,
            'Остаток' : None,
            'Маржа' : margin,
            'Параметр: Размер скидки' : None,
            'Параметр: Метки' : markers,
            'Параметр: Производитель' : None,
            'Параметр: Страна-производитель' : None,
            'Ссылка на товар' : response.url,
            "Ссылка на категорию товаров" : kwargs.pop('Ссылка на категорию товаров'),
            'Изображения' : images,
            'Параметр: Тип продукта': None,
            'Параметр: Group' : None,
            'Параметр: Артикул поставщика' : None,
        }
        exceptions = kwargs.pop('Исключения')
        for key, val in {
            ccs.find('div').text.strip() : ccs.find('div', class_='text-end').text.strip()
                for ccs in soup.find_all('div', class_='d-flex align-items-end justify-content-between mb-3')}.items():
                    match key:
                        case 'Бренд':
                            if val.casefold() in exceptions:
                                return 
                            result['Параметр: Бренд'] = val
                            result['Параметр: Производитель'] = val
                        case 'Артикул':
                            continue
                        case "Страна происхождения":
                            result['Параметр: Страна-производитель'] = val
                        case _:
                            result['Параметр: {}'.format(key)] = val
       
        table = soup.find('div', class_='table-responsive')
        if table is not None:
            keys = [re.sub('\W', '', i.text) for i in table.thead.find_all('th')[:-2]]
            for product in table.tbody.find_all('tr'):
                title, old_price, price, article = None, None, None, None
                for prop, key in zip(product.find_all('td'), keys):
                    match key.casefold():
                        case "вариация":
                            try:
                                result["Свойство: Вариант"] = "https://krasniykarandash.ru" + prop.a['href']
                            except:
                                result['Свойство: Вариант'] = None
                        case 'цена':
                            if prop.find(class_='price-current') is not None:
                                price = re.sub(r'\D', '', prop.find(class_='price-current').text)
                            if prop.find(class_='price-old') is not None:
                                old_price  = re.sub(r'\D', '', prop.find(class_='price-old').text)
                        case 'артикул':
                            article = prop.text.strip()
                            result['Артикул'] = prefix + article 
                            result['Артикул поставщика'] = article
                            result['Параметр: Артикул поставщика'] = article
                        case "наименование":
                            result['Название товара или услуги'] = prop.text.strip()
                        case _:
                            result[f'Параметр: {key}'] = prop.text.strip()
                eval_price = eval(formulae.replace('ЦЗ', str(price).replace(',', '.')).replace('р','').replace('МАРЖА', str(margin))) if price is not None else None
                result['Цена закупки'] = "{:.2f}".format(eval_price).replace('.', ',') if eval_price is not None else None
                result['Цена продажи'] = price.replace('.', ',') if price is not None else None
                result['Старая цена'] = old_price.replace('.', ',') if old_price is not None else None
                yield {**result, **kwargs}
        else:
            title = soup.find('h1').text.strip()
            article = soup.find('div', string=re.compile(r'Артикул')).find_next('div', class_='text-end').text.strip()
            result['Название товара или услуги'] = title
            result['Артикул'] = prefix + article 
            result['Артикул поставщика'] = article
            result['Параметр: Артикул поставщика'] = article
            soup_price = soup.find('span', class_='current')
            if soup_price is not None:
                price = re.sub(r'[^0-9.,]', '', soup_price.text.strip())[:-1]
            else:
                price = None
            soup_old_price = soup.find('span', class_='old')
            if soup_old_price is not None:
                old_price = re.sub(r'[^0-9.,]', '', soup_old_price.text.strip())[:-1]
            else:
                old_price = None
            eval_price = eval(formulae.replace('ЦЗ', str(price).replace(',', '.')).replace('р','').replace('МАРЖА', str(margin))) if price is not None else None
            result['Цена закупки'] = "{:.2f}".format(eval_price).replace('.', ',') if eval_price is not None else None
            result['Цена продажи'] = price.replace('.', ',') if price is not None else None
            result['Старая цена'] = old_price.replace('.', ',') if old_price is not None else None
            yield {**result, **kwargs}

       

        
        

    def catalog(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find(id='js-catalog-container')
        products = table.find_all('div', class_='prod__item js-prod-item')
        selected_page = table.find('a', class_='pagination__item selected')
        if selected_page is not None:
            next_page = selected_page.find_next('a', class_='pagination__item')
            if next_page is not None:
                next_page = "https://krasniykarandash.ru" + next_page['href']
                yield scrapy.Request(
                    url = next_page,
                    callback = self.catalog,
                    cb_kwargs = kwargs
                )
        
        for product in products:
            yield scrapy.Request(
                url = "https://krasniykarandash.ru" + product.a['href'] + "?SHOWALL_1=1",
                callback = self.product,
                cb_kwargs = kwargs
            )

    def closed(self, reason):
        with open('red/red.jsonl', 'r', encoding='utf-8') as file:
            s = file.readlines()
        result = [json.loads(item) for item in s]
        p = pd.DataFrame(result)
        with pd.ExcelWriter('red/red.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
            p.to_excel(writer, index=False, sheet_name='result')
            main_headers = [
                'Название товара или услуги',
                'Цена закупки',
                'Старая цена',
                'Артикул',
                'Параметр: Размер скидки',
                'Параметр: Остаток',
                'Цена продажи',
                'Параметр: Поставщик',
                'Параметр: Group'
            ]
            r = []
            for item in result:
                tmp = {}
                for key in item.keys():
                    if key in main_headers:
                        tmp[key] = item[key]
                r.append(tmp)
            p = pd.DataFrame(r)
            p.to_excel(writer, sheet_name='result_1', index=False)
            table = pd.read_excel('generalTable.xlsx', sheet_name='KARM').to_dict('list')
            for name, products in df(result, "Номер"):
                p = pd.DataFrame(products)
                table['Кол-во товаров'][name - 1] = len(products)
                table['Номер книги в файле'][name - 1] = name
                p.to_excel(writer, sheet_name=str(name), index=False)
            with pd.ExcelWriter('generalTable.xlsx', mode='a', if_sheet_exists='replace', engine='openpyxl') as w:
                pd.DataFrame(table).to_excel(w, sheet_name='KARM', index=False)



def run():
    with open ('red/ready.txt', 'w') as file:
        file.write('In Process')
    p = CrawlerProcess(Settings())
    p.crawl(RED)
    p.start()
    with open ('red/ready.txt', 'w') as file:
        file.write('Ready')