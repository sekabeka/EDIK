import scrapy
import lxml
import re
import json

import pandas as pd

from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings



def df(lst, key):
    result = {i : [] for i in set([i[key] for i in lst])}
    for item in lst:
        num = item[key]
        del item[key]
        result[num].append(item)
    for k, v in result.items():
        yield k, v

class CHILD(scrapy.Spider):
    name = 'ChildWorld'
    custom_settings = {
        "FEEDS" : {
            'child/child.jsonl' : {
                'format' : 'jsonlines',
                'encoding' : 'utf-8',
                'overwrite' : True
            }
        }, 
        "LOG_FILE" : 'child/child.log',
        'LOG_FILE_APPEND' : False,
        'DOWNLOAD_FAIL_ON_DATALOSS' : False,
        "DOWNLOADER_MIDDLEWARES" : {
            'child.proxymiddlewares.ProxyRegister' : 543
        }, 
        "CONCURRENT_ITEMS" : 200,
        #"AUTOTHROTTLE_ENABLED" : True,
        #'AUTOTHROTTLE_START_DELAY' : 0,
        #'AUTOTHROTTLE_DEBUG' : True,
        #"AUTOTHROTTLE_MAX_DELAY" : 0.1,
          
    }
    def start_requests(self):
        p = pd.read_excel('generalTable.xlsx', sheet_name='DETI').to_dict('list')
        start_urls = p['Ссылки на категории товаров']
        roots_categories = p['Корневая']
        add_categories, add2_categories = p['Подкатегория 1'], p['Подкатегория 2']
        placements = p['Размещение на сайте']
        prefixs = p['Префиксы']
        fms = p["Формула цены продажи"]
        mrgs = p['Маржа']
        deliveries = p['Параметр: Доставка']
        exceptions = [i.casefold().strip() for i in list(filter(lambda x: type(x) == str, p['Исключения']))]
        value = 1
        for url, root, add, add2, pref, place, fm, mg, delivery in zip(start_urls, roots_categories, add_categories, add2_categories, prefixs, placements, fms, mrgs, deliveries):
            kwargs = {
                'Корневая' : root,
                'Подкатегория 1' : add,
                'Подкатегория 2' : add2 if add2 else None,
                'Префикс' : pref,
                'Расположение на сайте' : place,
                'Номер' : value,
                "init" : None,
                'Формула' : fm.split('=')[-1],
                'Маржа' : mg,
                'Доставка' : delivery,
                'Ссылка на категорию товаров' : url,
                'Исключения' : exceptions
            }
            yield scrapy.Request(
                url,
                cb_kwargs=kwargs,
                dont_filter=True
            )
            value = value + 1

    


    def ReceiveInfo(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        brand = soup.find(attrs={'data-testid' : 'moreProductsItem'}).a.text.strip()
        exceptions = kwargs.pop('Исключения')
        if brand.casefold() in exceptions:
            return 
        title = soup.find('h1', attrs={'data-testid' : 'productTitle'}).text.strip()
        div_contain_sections = soup.find('div', attrs={'data-testid' : 'productSections'})
        formulae = kwargs.pop('Формула')
        margin = kwargs.pop('Маржа')
        sale_price = None
        for count, section in enumerate(div_contain_sections.find_all('section', recursive=False)):
            match count:
                case 0:
                    pictures = section.find_all("picture")
                    images = []
                    try:
                        for item in [i.source["srcset"] for i in pictures]:
                            images.append(re.search(r'(.+?webp)', item)[0])
                    except:
                        for item in [i.img["src"] for i in pictures]:
                            images.append(item)
                    images = ' '.join(images)
                case 1:
                    ul = section.find('ul')
                    if ul:
                        ul = ul.find_all('li')
                        markers = ' '.join([li.text for li in ul])
                    else:
                        markers = None
                case 2:
                    if section.find('p', attrs={'data-testid' : 'price'}):
                        price = re.sub(r'[^,\.0-9]','',section.find('p', attrs={'data-testid' : 'price'}).text)
                        if '%' in section.find('p', attrs={'data-testid' : 'price'}).find_next().text:
                            sale_size = re.sub('\D', '', section.find('p', attrs={'data-testid' : 'price'}).find_next().text)
                        else:
                            sale_size = None
                    else:
                        price = 'Нет в наличии'
                        sale_size = None
                case 3:
                    description = section.find('section', attrs={'data-testid' : 'descriptionBlock'})
                    if description:
                        description = re.sub(r'\xa0', ' ', description.div.text.strip())
                    else:
                        description = None
                    characteristic = section.find('section', attrs={'data-testid' : 'characteristicBlock'})
                    tmp = {}
                    if characteristic:
                        table = characteristic.table
                        for it in table.find_all('tr'):
                            match it.th.text.strip().lower():
                                case 'артикул':
                                    article = it.td.text.strip()
                                    continue
                                case 'страна производства':
                                    name, prop = (f'Параметр: Страна-производитель', it.td.text.strip())
                                case 'продавец':
                                    continue
                                case 'вес упаковки, кг':
                                    mass = float(it.td.text.strip())
                                    sale_price = eval(formulae.replace('ЦЗ', str(price).replace(',', '.')).replace('ВЕС', str(mass)).replace('р','').replace('МАРЖА', str(margin))) if price != 'Нет в наличии' else None
                                case 'тип продукта':
                                    #name, prop = ('Вес', it.td.text.strip().replace('.', ','))
                                    pass
                                case _ :
                                    name, prop = (f'Параметр: {it.th.text.strip()}', it.td.text.strip())
                            tmp[name] = prop
                    else:
                        pass
        return {
            'Параметр: Поставщик' : "DETI",
            "Параметр: Доставка" :  kwargs.pop('Доставка'),
            'Свойство: Вариант' : kwargs.pop("Свойство: Вариант") if "Свойство: Вариант" in kwargs.keys() else None,
            "Вес, кг" : str(mass).replace('.', ',') if sale_price is not None else 'Нет массы',
            'Корневая' : kwargs.pop('Корневая'),
            'Подкатегория 1' : kwargs.pop('Подкатегория 1'),
            "Подкатегория 2" : kwargs.pop('Подкатегория 2'),
            'Название товара или услуги' : title,
            'Полное описание' : description,
            'Краткое описание' : None,
            'Параметр: Бренд' : brand,
            'Размещение на сайте' : kwargs.pop('Расположение на сайте'),
            'Артикул' : kwargs.pop('Префикс') + tmp['Параметр: Код товара'],
            "Артикул поставщика" : tmp['Параметр: Код товара'],
            'Цена продажи' : '{:.2f}'.format(sale_price).replace('.', ',') if sale_price is not None else None,
            'Старая цена' : format(float((1 + int(sale_size) / 100) * 1.6 * float(price.replace(',', '.'))), '.2f').replace('.', ',') if price != 'Нет в наличии' and sale_size != None and sale_size else None,
            'Цена закупки' : price.replace('.', ','),
            'Остаток' : 100 if price != 'Нет в наличии' else 0,
            'Маржа' : margin,
            'Параметр: Размер скидки' : sale_size,
            'Параметр: Метки' : markers,
            'Параметр: Производитель' : brand,
            'Параметр: Страна-производитель' : tmp['Параметр: Страна-производитель'] if "Параметр: Страна-производитель" in tmp.keys() else None,
            'Ссылка на товар' : response.url,
            "Ссылка на категорию товаров" : kwargs.pop('Ссылка на категорию товаров'),
            'Изображения' : images,
            'Параметр: Тип продукта': None,
            'Параметр: Group' : None,
            'Параметр: Артикул поставщика' : article,
            **tmp, **kwargs
        }
        
    def handler(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        kwargs.pop("init")
        if 'page' and 'domain' in kwargs.keys():
            kwargs.pop('page')
            kwargs.pop('domain')
        if soup.find('div', attrs={'data-testid' : 'variantsBlock'}):
            if 'zoozavr' in response.url:
                variants = [('https://www.zoozavr.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            else:
                variants = [('https://www.detmir.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            for url, var in variants:
                if url != response.url:
                    kwargs["Свойство: Вариант"] = var
                    yield scrapy.Request(url, callback=self.ReceiveInfo, cb_kwargs=kwargs)
                else:
                    kwargs["Свойство: Вариант"] = var
                    yield self.ReceiveInfo(response=response, **kwargs)
        else:
            yield self.ReceiveInfo(response=response, **kwargs)
            
    def parse(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')  
        #domain, page = kwargs['domain'], kwargs['page']  
        if 'zoo' in response.url:
            products = soup.find_all('section', id=re.compile(r'product-\d+'))
            for prod in products:
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    return
                link = prod.find('a')['href']
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        else:
            products = soup.find_all('section', id=re.compile(r'\d+'))
            for prod in products:
                link = prod.find(href=re.compile(r'.*?www\.detmir\.ru.*'))['href']
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    return
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        
        #if soup.find(string=re.compile(r"показать ещё", flags=re.I)):
            #new_url = domain + f'page/{page + 1}'
            #kwargs['page'] += 1
            #yield scrapy.Request(new_url, callback=self.parse, cb_kwargs=kwargs)
        if kwargs["init"] is None:
            pagination_tag = soup.find('nav', attrs={'aria-label': "pagination"})
            kwargs["init"] = True
            if pagination_tag is not None:
                max_page = int(pagination_tag.find_all("li")[-1].text)
                for idx in range(2, max_page + 1):
                    yield scrapy.Request(response.url + '?page={}'.format(idx), callback=self.parse, cb_kwargs=kwargs)
    
    def closed(self, reason):
        with open('child/child.jsonl', 'r', encoding='utf-8') as file:
            s = file.readlines()
        result = [json.loads(item) for item in s]
        p = pd.DataFrame(result)
        with pd.ExcelWriter('child/child.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
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
            table = pd.read_excel('generalTable.xlsx', sheet_name='DETI').to_dict('list')
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
                pd.DataFrame(table).to_excel(w, sheet_name='DETI', index=False)
          

def run():
    with open ('child/ready.txt', 'w') as file:
        file.write('In Process')
    p = CrawlerProcess(Settings())
    p.crawl(CHILD)
    p.start()
    with open ('child/ready.txt', 'w') as file:
        file.write('Ready')
