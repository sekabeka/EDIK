import scrapy
import re

from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from fake_useragent import UserAgent

import pandas as pd



def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v




def _filter(object:pd.DataFrame, key:str):
    return object.drop_duplicates(subset=[key])

class AUCHAN(scrapy.Spider):
    name = 'scraper'
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES" : {
            'auchan.middlewares.CookiesAuchan' : 300,
        },
        'LOG_FILE' : 'auchan/auchan.log',
        'LOG_FILE_APPEND' : False,
        'REQUEST_FINGERPRINTER_IMPLEMENTATION' : "2.7",
        "FEEDS" : {
            "auchan/auchan.jsonl" : {
                "format" : "jsonlines",
                "encoding" : "utf-8",
                "overwrite" : True
            }
        },
        'ROBOTSTXT_OBEY' : False,
        #'USER_AGENT' : UserAgent().random
    }

    def start_requests(self):
        p = pd.read_excel('generalTable.xlsx', sheet_name='SMDB').to_dict('list')
        placement = p['Размещение на сайте']
        urls = p['Ссылки на категории товаров']
        prefixs = p['Префиксы']
        roots = p['Корневая']
        cats1 = p['Подкатегория 1']
        cats2 = p['Подкатегория 2']
        fms = p["Формула цены продажи"]
        mrgs = p['Маржа']
        deliveries = p['Параметр: Доставка']
        exceptions = [i.casefold().strip() for i in list(filter(lambda x: type(x) == str, p['Исключения']))]
        idx = 1
        for (
            place, url, pref, cat1, cat2, fm, mg, delivery, root
        ) in zip(
            placement, urls, prefixs, cats1, cats2, fms, mrgs, deliveries, roots
        ):
            yield scrapy.Request(
                url=url,
                cb_kwargs={
                    'Корневая': root,
                    'Подкатегория 1' : cat1,
                    'Подкатегория 2' : cat2,
                    'Префикс' : pref,
                    'Размещение на сайте' : place,
                    'Формула' : fm.split('=')[-1],
                    'Маржа' : mg,
                    'Параметр: Доставка' : delivery,
                    'Ссылка на категорию товаров' : url,
                    'Номер' : idx,
                    'Исключения' : exceptions
                },
                callback=self.catalogs,
                dont_filter=True
            )
            idx = idx + 1
    

    def catalogs(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        products = soup.find_all('div', class_=re.compile(r'Layout'))[1].find_all('article', class_=re.compile(r'productCard active'))
        links = ['https://www.auchan.ru' + item.find('a', class_='productCardPictureLink')['href'] for item in products]
        for link in links:
            yield scrapy.Request(
                url=link,
                cb_kwargs=kwargs
            )
        next_page = soup.find('li', class_='pagination-arrow pagination-arrow--right')
        if next_page is not None:
            next_page = "https://www.auchan.ru" + next_page.a['href']
            yield scrapy.Request(
                url=next_page,
                cb_kwargs=kwargs,
                callback=self.catalogs
            )


    def parse(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        brand = soup.find('a', class_='css-1awj3d7')
        exceptions = kwargs.pop('Исключения')
        if brand != None:
            brand = brand.text.strip()
            if brand.casefold() in exceptions:
                return 
        else:
            brand = "Не указан"
        price = soup.find('div', class_='fullPricePDP')
        if price != None :
            price = re.sub(r',', '.', re.sub(r'[^0-9,]', '', price.text.strip()))
        else:
            price = "Нет в наличии"
        old_price = soup.find('div', class_='oldPricePDP')
        count = soup.find('span', class_='inStockData')
        if count != None:
            count = re.sub(r'\D', '', count.text.strip())
        else:
            count = 0
        sale = soup.find('span', class_='css-acbihw')
        if sale != None:
            if price != "Нет в наличии":
                old_price = format(float(float(price) * 2 * (1 + int(sale.text.strip()) / 100)),'.2f').replace('.', ',')
            else:
                old_price = None
            sale = sale.text.strip() + "%"
        else:
            sale = "Не указана"
        period_sale = soup.find('div', class_='css-1aty3d4')
        if period_sale != None:
            period_sale = 'до ' + re.sub(r'[^0-9.]', '', period_sale.text.strip())
        else:
            period_sale = 'Не указана'
        try:
            definition = re.compile(
                r'({"content":"(.*?)"})'
            ).search(response.text)[2]
        except:
            definition = 'Нет описания'
        # if definition != None:
        #     definition = re.sub(r'\W', ' ', definition.text)
        # else:
        #     definition = 'Нет описания'
        images_divs = soup.find_all('div', class_='swiper-slide')
        images = []
        try:
            for item in images_divs:
                attrs = item.img.attrs
                for key in attrs.keys():
                    if re.compile('src').search(key) != None:
                        images.append(
                            'https://www.auchan.ru' + attrs[key]
                        )
                    else:
                        continue
        except Exception as e:
            pass
        table = soup.find('table', class_='css-9qtgi1')
        if table != None:
            article = table.find('td', class_='css-em38yw')
            if article != None:
                article = re.sub(r'\D', '', article.text)
            else:
                pass
        name = soup.find('h1', id='productName').text.strip()
        
        tmp = {}
        for item in soup.find('table', class_='css-9qtgi1').find_all('tr'):
            prop = item.find('th').text.strip()
            key = item.find('td').text.strip()
            tmp[prop] = key
        properties = {}
        names = [
            'Страна производства',
            "Тип товара",
            "Область применения",
            "Пол",
            "Эффект от использования",
            "Назначение",
            'Тип крупы',  
        ]
        for name in names:
            for key in tmp.keys():
                if name == key:
                    properties[f'Параметр: {name}'] = tmp[key]
                    break
                else:
                    continue
        prefix = kwargs.pop('Префикс')
        formulae = kwargs.pop('Формула')
        margin = kwargs.pop('Маржа')
        pattern = re.compile(r"Масса брутто, кг")
        if soup.find(string=pattern) is not None:
            mass = float(soup.find(string=pattern).find_next().text)
        else:
            mass = 'Нет массы'
        result = {
            'Параметр: Поставщик' : 'SMDB',
            "Параметр: Доставка" :  kwargs.pop('Параметр: Доставка'),
            "Свойство: Вариант" : None,
            'Вес, кг' : mass,
            'Корневая' : kwargs.pop('Корневая'),
            'Подкатегория 1' : kwargs.pop('Подкатегория 1'),
            "Подкатегория 2" : kwargs.pop('Подкатегория 2'),
            'Название товара или услуги' : name,
            'Полное описание' : definition,
            'Краткое описание' : None,
            'Параметр: Бренд' : brand,
            "Размещение на сайте" : kwargs.pop('Размещение на сайте'),
            'Артикул' : str(prefix) + article,
            'Артикул поставщика' : article,
            'Цена продажи' : "{:.2f}".format(eval(formulae.replace('ЦЗ', str(price)).replace('ВЕС', str(mass)).replace('р','').replace('МАРЖА', str(margin)))).replace('.', ',') if type(mass) == float else None,
            'Старая цена' : old_price,
            'Цена закупки' : re.sub('[.]',',',price),
            'Остаток' : count,
            "Маржа" : margin,
            'Параметр: Размер скидки' : sale,
            "Параметр: Метки" : None,
            'Параметр: Производитель' : brand,
            'Параметр: Страна-производитель' : properties.pop('Параметр: Страна производства') if 'Параметр: Страна производства' in properties.keys() else None,
            'Ссылка на товар' : response.url,
            'Ссылка на категорию товаров' : kwargs.pop("Ссылка на категорию товаров"),
            'Изображения' : ' '.join(images),
            'Параметр: Артикул поставщика' : article,
            'Параметр: Период скидки' : period_sale,
            'Параметр: Group' : str(prefix)[:-1].upper(),
            **properties, **kwargs
        }
        
        yield result

    def closed(self, reason):
        import json
        bad_brands = [
           i.upper() for i in  pd.read_excel('auchan/brands.xlsx').to_dict('list')[0]
        ]
        with open('auchan/auchan.jsonl', encoding='utf-8', mode='r') as file:
            s = file.readlines()
        result = [json.loads(i) for i in s]
        for item in result:
            if item['Параметр: Бренд'].upper() in bad_brands:
                del item
            else:
                continue
        with pd.ExcelWriter('auchan/auchan_result.xlsx', mode='w', engine_kwargs={'options': {'strings_to_urls': False}}, engine='xlsxwriter') as writer:
            keys = [
                'Название товара или услуги',
                'Артикул',
                'Старая цена',
                'Остаток',
                'Цена закупки',
                'Цена продажи',
                'Параметр: Group',
                'Параметр: Поставщик'
            ]
            key = 'Параметр: Артикул поставщика'
            p = pd.DataFrame(result)
            p = _filter(p,key)
            tmp = p.to_dict('list')
            for key in list(tmp.keys()):
                if key in keys:
                    pass
                else:
                    tmp.pop(key)
            p.to_excel(writer, index=False, sheet_name='result')
            pd.DataFrame(tmp).to_excel(writer, index=False, sheet_name='result_1')
            table = pd.read_excel('generalTable.xlsx', sheet_name='SMDB').to_dict('list')
            value = 0
            for name, products in df(result, "Номер"):
                p = pd.DataFrame(products)
                value += len(products)
                table['Кол-во товаров'][name - 1] = len(products)
                table['Номер книги в файле'][name - 1] = name
                p.to_excel(writer, sheet_name=str(name), index=False)
            for key in table:
                if key == 'Кол-во товаров':
                    table[key].append(value)
                else:
                    table[key].append(None)
            with pd.ExcelWriter('generalTable.xlsx', mode='a', if_sheet_exists='replace', engine='openpyxl') as w:
                pd.DataFrame(table).to_excel(w, sheet_name='SMDB', index=False)
            
            
            

def run():
    with open ('auchan/ready.txt', 'w') as file:
        file.write('In Process')
    p = CrawlerProcess(Settings())
    p.crawl(AUCHAN)
    p.start()
    with open ('auchan/ready.txt', 'w') as file:
        file.write('Ready')
