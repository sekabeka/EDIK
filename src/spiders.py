import scrapy
import re
import lxml
import json
import pandas as pd

from bs4 import BeautifulSoup
from scrapy.crawler import Crawler

from src.functions import df, filter
from src.items import Product

LOG_FILE_APPEND = False
CONCURRENT_ITEMS = 500
CONCURRENT_REQUESTS = 250
CONCURRENT_REQUESTS_PER_DOMAIN = None

PATH_TO_GENERAL_TABLE = 'generalTable.xlsx'
PATH_TO_LOG_DIRECTORY = 'src/logs'
PATH_TO_RESULT_DIRECTORY = 'src/results'


class auchanScraper(scrapy.Spider):
    name = 'scraper_auchan'
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES" : {
            'src.middlewares.AuchanMiddlewares' : 1,
        },
        'LOG_FILE' : f'{PATH_TO_LOG_DIRECTORY}/auchan.log',
        'LOG_FILE_APPEND' : False,
        "FEEDS" : {
            f"{PATH_TO_RESULT_DIRECTORY}/auchan.jsonl" : {
                "format" : "jsonlines",
                "encoding" : "utf-8",
                "overwrite" : True
            }
        },
        'ROBOTSTXT_OBEY' : False,
        "CONCURRENT_ITEMS" : CONCURRENT_ITEMS,
        "CONCURRENT_REQUESTS" : CONCURRENT_REQUESTS
        #'USER_AGENT' : UserAgent().random
    }

    def __init__(self, proxylist=[]):
        self.proxylist = proxylist
    
    @classmethod
    def from_crawler(cls, crawler: Crawler, *args, **kwargs):
        return super().from_crawler(crawler, *args, **kwargs)

    def start_requests(self):
        p = pd.read_excel(PATH_TO_GENERAL_TABLE, sheet_name='SMDB').to_dict('list')
        placement = p['Размещение на сайте']
        urls = p['Ссылки на категории товаров']
        prefixs = p['Префиксы']
        cats1 = p['Подкатегория 1']
        cats2 = p['Подкатегория 2']
        fms = p["Формула цены продажи"]
        mrgs = p['Маржа']
        deliveries = p['Параметр: Доставка']
        idx = 1
        for (
            place, url, pref, cat1, cat2, fm, mg, delivery
        ) in zip(
            placement, urls, prefixs, cats1, cats2, fms, mrgs, deliveries
        ):
            yield scrapy.Request(
                url=url,
                cb_kwargs={
                    'Подкаталог 1' : cat1,
                    'Подкаталог 2' : cat2,
                    #'Подкаталог 3' : cat3 if cat3 else None,
                    'prefix' : pref,
                    'Размещение на сайте' : place,
                    'Формула' : fm.split('=')[-1],
                    'Маржа' : mg,
                    'Параметр: Доставка' : delivery,
                    'Ссылка на категорию товаров' : url,
                    'number' : idx

                },
                callback=self.catalogs
            )
            idx = idx + 1

    def handler(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        products = soup.find_all('div', class_=re.compile(r'Layout'))[1].find_all('article', class_=re.compile(r'productCard active'))
        for url in ['https://www.auchan.ru' + item.find('a', class_='productCardPictureLink')['href'] for item in products]:
            yield scrapy.Request(
                url=url,
                cb_kwargs=kwargs
            )

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
        if brand != None:
            brand = brand.text.strip()
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
        tmp = []
        prefix = kwargs.pop('prefix')
        formulae = kwargs.pop('Формула')
        margin = kwargs.pop('Маржа')
        pattern = re.compile(r"Масса брутто, кг")
        if soup.find(string=pattern) is not None:
            mass = float(soup.find(string=pattern).find_next().text)
        else:
            mass = 'Нет массы'
        result = {
            'Вес, кг' : mass,
            **kwargs,
            'Название товара или услуги' : name,
            'Полное описание' : definition,
            'Краткое описание' : None,
            'Артикул' : str(prefix) + article,
            'Цена продажи' : "{:.2f}".format(eval(formulae.replace('ЦЗ', str(price)).replace('ВЕС', str(mass)).replace('р','').replace('МАРЖА', str(margin)))).replace('.', ',') if type(mass) == float else None,
            'Старая цена' : old_price,
            'Цена закупки' : re.sub('[.]',',',price),
            'Остаток' : count,
            'Параметр: Бренд' : brand,
            'Параметр: Артикул поставщика' : article,
            'Параметр: Производитель' : brand,
            'Параметр: Размер скидки' : sale,
            'Параметр: Период скидки' : period_sale,
            'Параметр: Поставщик' : 'SMDB',
            'Параметр: Group' : str(prefix)[:-1].upper(),
        }
        tmp = {}
        for item in soup.find('table', class_='css-9qtgi1').find_all('tr'):
            prop = item.find('th').text.strip()
            key = item.find('td').text.strip()
            tmp[prop] = key
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
            result[f'Параметр: {name}'] = None
            for key in tmp.keys():
                if name == key:
                    result[f'Параметр: {name}'] = tmp[key]
                    break
                else:
                    continue
        result['Изображения'] = ' '.join(images)
        result['Ссылка на товар'] = response.url
        yield result

    def closed(self, reason):
        with open(f'{PATH_TO_RESULT_DIRECTORY}/auchan.jsonl', encoding='utf-8', mode='r') as file:
            result = [json.loads(i) for i in file.readlines()]
            
        for process in self.processes:
            process.kill()

        with pd.ExcelWriter(f'{PATH_TO_RESULT_DIRECTORY}/auchan_result.xlsx', mode='w', engine_kwargs={'options': {'strings_to_urls': False}}, engine='xlsxwriter') as writer:
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
            p = filter(p,key)
            tmp = p.to_dict('list')
            for key in list(tmp.keys()):
                if key in keys:
                    pass
                else:
                    tmp.pop(key)
            p.to_excel(writer, index=False, sheet_name='result')
            pd.DataFrame(tmp).to_excel(writer, index=False, sheet_name='result_1')
            for name, res in df(result, 'number'):
                p = pd.DataFrame(res)
                p.to_excel(writer, sheet_name=str(name), index=False)
            
class detmirScraper(scrapy.Spider):
    name = 'parser_detmir'
    custom_settings = {
        "FEEDS" : {
            f'{PATH_TO_RESULT_DIRECTORY}/child.jsonl' : {
                'format' : 'jsonlines',
                'encoding' : 'utf-8',
                'overwrite' : True
            }
        }, 
        "LOG_FILE" : f'{PATH_TO_LOG_DIRECTORY}/child.log',
        'LOG_FILE_APPEND' : False,
        'DOWNLOAD_FAIL_ON_DATALOSS' : False,
        "DOWNLOADER_MIDDLEWARES" : {
            'src.middlewares.DetmirMiddlewares' : 1
        }, 
        "CONCURRENT_ITEMS" : CONCURRENT_ITEMS,
        "CONCURRENT_REQUESTS" : CONCURRENT_REQUESTS
    }

    def __init__(self, proxylist=[]):
        self.proxylist = proxylist
    
    @classmethod
    def from_crawler(cls, crawler: Crawler, *args, **kwargs):
        return super().from_crawler(crawler, *args, **kwargs)

    def start_requests(self):
        p = pd.read_excel(PATH_TO_GENERAL_TABLE, sheet_name='DETI').to_dict('list')
        start_urls = p['Ссылки на категории товаров']
        roots_categories = p['Корневая']
        add_categories, add2_categories = p['Подкатегория 1'], p['Подкатегория 2']
        placements = p['Размещение на сайте']
        prefixs = p['Префиксы']
        fms = p["Формула цены продажи"]
        mrgs = p['Маржа']
        deliveries = p['Параметр: Доставка']
        value = 1
        for url, root, add, add2, pref, place, fm, mg, delivery in zip(start_urls, roots_categories, add_categories, add2_categories, prefixs, placements, fms, mrgs, deliveries):
            kwargs = {
                'root_category' : root,
                'add_category' : add,
                'add2_category' : add2 if add2 else None,
                'prefix' : pref,
                'placement' : place,
                'number' : value,
                "init" : None,
                'Формула' : fm.split('=')[-1],
                'Маржа' : mg,
                'Доставка' : delivery,
                'Ссылка на категорию товаров' : url
            }
            yield scrapy.Request(
                url,
                cb_kwargs=kwargs,
            )
            value = value + 1

    def ReceiveInfo(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        brand = soup.find(attrs={'data-testid' : 'moreProductsItem'}).a.text.strip()
        title = soup.find('h1', attrs={'data-testid' : 'pageTitle'}).text.strip()
        div_contain_sections = soup.find('div', attrs={'data-testid' : 'productSections'})
        formulae = kwargs.pop('Формула')
        margin = kwargs.pop('Маржа')
        delivery = kwargs.pop('Доставка')
        sale_price = None
        url = kwargs.pop('Ссылка на категорию товаров')
        for count, section in enumerate(div_contain_sections.find_all('section', recursive=False)):
            match count:
                case 0:
                    pictures = section.find_all("picture")
                    images = []
                    for item in [i.source["srcset"] for i in pictures]:
                        images.append(re.search(r'(.+?webp)', item)[0])
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
            'Свойство: Вариант' : kwargs.pop("Свойство: Вариант") if "Свойство: Вариант" in kwargs.keys() else None,
            "Вес, кг" : str(mass).replace('.', ',') if sale_price is not None else 'Нет массы',
            "Параметр: Доставка" : delivery,
            "Ссылка на категорию товаров" : url,
            'Корневая' : kwargs.pop('root_category'),
            'Подкатегория 1' : kwargs.pop('add_category'),
            "Подкатегория 2" : kwargs.pop('add2_category'),
            'Артикул' : kwargs.pop('prefix') + tmp['Параметр: Код товара'],
            'Параметр: Тип продукта': None,
            'Параметр: Поставщик' : "DETI",
            'Параметр: Group' : None,
            'Название товара или услуги' : title,
            'Размещение на сайте' : kwargs.pop('placement'),
            'Полное описание' : description,
            'Ссылка на товар' : response.url,
            'Цена продажи' : '{:.2f}'.format(sale_price).replace('.', ',') if sale_price is not None else None,
            'Старая цена' : format(float((1 + int(sale_size) / 100) * 1.6 * float(price.replace(',', '.'))), '.2f').replace('.', ',') if price != 'Нет в наличии' and sale_size != None and sale_size else None,
            'Цена закупки' : price.replace('.', ','),
            'Изображения' : images,
            'Остаток' : 100 if price != 'Нет в наличии' else 0,
            'Параметр: Бренд' : brand,
            'Параметр: Производитель' : brand,
            'Параметр: Артикул поставщика' : article,
            'Параметр: Размер скидки' : sale_size,
            'Параметр: Метки' : markers,
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
                    self.logger.error(f'We have no available products {response.url, products.index(prod)}')
                    return
                link = prod.find('a')['href']
                yield scrapy.Request(link, callback=self.handler, cb_kwargs=kwargs)
        else:
            products = soup.find_all('section', id=re.compile(r'\d+'))
            for prod in products:
                link = prod.find(href=re.compile(r'.*?www\.detmir\.ru.*'))['href']
                if prod.find(string=re.compile(r'Товар закончился|Только в розничных магазинах')):
                    self.logger.error(f'We have no available products {response.url, products.index(prod)}')
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
        with open(f'{PATH_TO_RESULT_DIRECTORY}/child.jsonl', 'r', encoding='utf-8') as file:
            result = [json.loads(item) for item in file.readlines()]

        p = pd.DataFrame(result)
        with pd.ExcelWriter(f'{PATH_TO_RESULT_DIRECTORY}/child.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
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
            table = pd.read_excel(PATH_TO_GENERAL_TABLE, sheet_name='DETI').to_dict('list')
            for name, products in df(result, "number"):
                p = pd.DataFrame(products)
                table['Кол-во товаров'][name - 1] = len(products)
                table['Номер книги в файле'][name - 1] = name
                p.to_excel(writer, sheet_name=str(name), index=False)
            with pd.ExcelWriter(PATH_TO_GENERAL_TABLE, mode='a', if_sheet_exists='replace', engine='openpyxl') as w:
                pd.DataFrame(table).to_excel(w, sheet_name='DETI', index=False)
          
