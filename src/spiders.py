import scrapy
import re
import lxml

from bs4 import BeautifulSoup
from scrapy.crawler import Crawler

from src.functions import get_input_table_values

LOG_FILE_APPEND = False
CONCURRENT_ITEMS = 200
CONCURRENT_REQUESTS = 25
CONCURRENT_REQUESTS_PER_DOMAIN = None

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
    }

    def __init__(self, proxylist=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxylist = proxylist
    
    @classmethod
    def from_crawler(cls, crawler: Crawler, *args, **kwargs):
        return super().from_crawler(crawler, *args, **kwargs)

    def start_requests(self):
        for (
            root,
            subcategory1,
            subcategory2,
            url,
            breadcrumbs,
            formula,
            constraints
        ) in get_input_table_values('auch'):
            yield scrapy.Request(
                url=url,
                cb_kwargs={
                    'root' : root,
                    'subcategory1' : subcategory1,
                    'subcategory2' : subcategory2,
                    'breadcrumbs' : breadcrumbs,
                    'formula' : formula,
                    'constraints' : constraints,
                    'init' : None
                },
                callback=self.catalogs,
                dont_filter=True
            )

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
        product = {
            'Свойство: Вариант' : None,
            'Вес, кг' : '0.1',
            'Корневая' : kwargs.pop('root'),
            'Подкатегория 1' : kwargs.pop('subcategory1'),
            'Подкатегория 2' : kwargs.pop('subcategory2'),
            'Артикул' : None,
            'Название товара или услуги' : None,
            'Размещение на сайте' : kwargs.pop('breadcrumbs'),
            'Полное описание' : None,
            'Цена продажи' : None,
            'Старая цена' : None,
            'Себестоимость' : None,
            'Изображения' : None,
            'Остаток' : None,
            'Параметр: Ссылка на категорию товара' : None,
            'Параметр: Бренд' : None,
            'Параметр: Производитель' : None,
            'Параметр: Размер скидки' : None,
            'Параметр: Промо' : None,
            'Параметр: Метки' : None,
            'Параметр: Код товара' : None,
            'Параметр: Тип' : None,
            'Параметр: Страна-производитель' : None
        }

        
        soup = BeautifulSoup(response.text, 'lxml')
        breadcrumbs = ' > '.join([i.text.strip() for i in soup.find_all("li", attrs={"itemprop" : "itemListElement"})[:-1]])
        product['Параметр: Ссылка на категорию товара'] = breadcrumbs

        table = soup.find('table', class_='css-9qtgi1')
        if table is not None:
            article = table.find('td', class_='css-em38yw')
            if article is not None:
                article = re.sub(r'\D', '', article.text)
                product['Артикул'] = 'BNA' + article
                product['Параметр: Код товара'] = article

        formula = kwargs.pop('formula')
        brand = soup.find('a', class_='css-1awj3d7')
        if brand is not None:
            brand = brand.text.strip()
            constraints = kwargs.pop('constraints')
            if constraints:
                if brand.casefold() in constraints.keys():
                    special_formula = constraints[brand.casefold()]
                    if special_formula.casefold() == 'stop':
                        return
                    formula = special_formula

                if product['Артикул'].casefold() in constraints.keys():
                    special_formula = constraints[product['Артикул'].casefold()]
                    if special_formula.casefold() == 'stop':
                        return
                    if '=' in special_formula:
                        formula = special_formula
                    else:
                        product['Цена продажи'] = special_formula
                        
        formula = formula.replace('(маржа)', '').replace(',', '.').replace('р', '').replace('ЦЗ', '%(purchase_price)f').replace('вес', '%(mass)f')
        product['Параметр: Бренд'] = brand
        product['Параметр: Производитель'] = brand
        purchase_price = soup.find('div', class_='fullPricePDP')
        if purchase_price is not None :
            purchase_price = float(re.sub(r',', '.', re.sub(r'[^0-9,]', '', purchase_price.text.strip())))
            pattern = re.compile(r"Масса брутто, кг")
            if soup.find(string=pattern) is not None:
                mass = float(soup.find(string=pattern).find_next().text)
                product['Вес, кг'] = str(mass).replace('.', ',')

            mass = float(product['Вес, кг'].replace(',', '.'))
            if product['Цена продажи'] is None:
                product['Цена продажи'] = str(
                    round(
                        eval(
                            formula % {'purchase_price': purchase_price, 'mass': mass}
                        ),
                        2
                    )
                ).replace('.', ',')
            product['Себестоимость'] = str(round(purchase_price, 2)).replace('.', ',')

        count = soup.find('span', class_='inStockData')
        if count is not None:
            count = re.sub(r'\D', '', count.text.strip())
        else:
            count = 0
        product['Остаток'] = count

        sale = soup.find('span', class_='css-acbihw')
        if sale is not None:
            sale = int(sale.text.strip())
            if purchase_price is not None:
                old_price_pattern = '%(purchase_price)f * 2 * (1 + %(sale)d / 100)'
                old_price = str(round(eval(old_price_pattern % {'purchase_price' : purchase_price, 'sale' : sale}), 2)).replace('.', ',')
            else:
                old_price = None
            product['Старая цена'] = old_price
            product['Параметр: Размер скидки'] = sale

        # period_sale = soup.find('div', class_='css-1aty3d4')
        # if period_sale != None:
        #     period_sale = 'до ' + re.sub(r'[^0-9.]', '', period_sale.text.strip())
        # else:
        #     period_sale = 'Не указана'
        try:
            definition = re.compile(
                r'({"content":"(.*?)"})'
            ).search(response.text)[2]
        except:
            pass
        else:
            product['Полное описание'] = definition
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
        product['Изображения'] = ' '.join(images)


        title = soup.find('h1', id='productName').text.strip()
        product['Название товара или услуги'] = title
        for item in soup.find('table', class_='css-9qtgi1').find_all('tr'):
            prop = item.find('th').text.strip()
            key = item.find('td').text.strip()
            match prop.casefold():
                case "страна производства":
                    product['Параметр: Страна-производитель'] = key
                case "тип товара":
                    product['Параметр: Тип'] = key.capitalize()
                case _:
                    pass
        return product
    
    def closed(self, reason):
        for p in self.processes:
            p.kill()
 
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

    def __init__(self, proxylist=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxylist = proxylist
    
    @classmethod
    def from_crawler(cls, crawler: Crawler, *args, **kwargs):
        return super().from_crawler(crawler, *args, **kwargs)

    def start_requests(self):
        for (
            root,
            subcategory1,
            subcategory2,
            url,
            breadcrumbs,
            formula,
            constraints
        ) in get_input_table_values('detmir'):
            yield scrapy.Request(
                url=url,
                cb_kwargs={
                    'root' : root,
                    'subcategory1' : subcategory1,
                    'subcategory2' : subcategory2,
                    'breadcrumbs' : breadcrumbs,
                    'formula' : formula,
                    'constraints' : constraints,
                    'init' : None
                },
                dont_filter=True
            )

    def ReceiveInfo(self, response, **kwargs):
        product = {
            'Свойство: Вариант' : kwargs.pop('variant', None),
            'Вес, кг' : 0.1,
            'Корневая' : kwargs.pop('root'),
            'Подкатегория 1' : kwargs.pop('subcategory1'),
            'Подкатегория 2' : kwargs.pop('subcategory2'),
            'Артикул' : None,
            'Название товара или услуги' : None,
            'Размещение на сайте' : kwargs.pop('breadcrumbs'),
            'Полное описание' : None,
            'Цена продажи' : None,
            'Старая цена' : None,
            'Себестоимость' : None,
            'Изображения' : None,
            'Остаток' : 0,
            'Параметр: Ссылка на категорию товара' : None,
            'Параметр: Бренд' : None,
            'Параметр: Производитель' : None,
            'Параметр: Размер скидки' : None,
            'Параметр: Промо' : None,
            'Параметр: Метки' : None,
            'Параметр: Код товара' : None,
            'Параметр: Тип' : None,
            'Параметр: Страна-производитель' : None
        }
        old_price_pattern = '(1 + %(sale)d / 100) * 1.6 * %(purchase_price)f'

        soup = BeautifulSoup(response.text, 'lxml')
        breadcrumbs = ' > '.join(
           [i.text.strip() for i in soup.find_all("li", attrs={"data-testid" : "breadcrumbsItem"})[:-1]]
        )
        product['Параметр: Ссылка на категорию товара'] = breadcrumbs
        div_contain_sections = soup.find('div', attrs={'data-testid' : 'productSections'})
        for count, section in enumerate(div_contain_sections.find_all('section', recursive=False)):
            match count:
                case 0:
                    pictures = section.find_all("picture")
                    images = []
                    for item in [i.source["srcset"] for i in pictures]:
                        images.append(re.search(r'(.+?webp)', item)[0])
                    images = ' '.join(images)
                    product['Изображения'] = images
                case 1:
                    ul = section.find('ul')
                    if ul:
                        ul = ul.find_all('li')
                        markers = ' '.join([li.text for li in ul])
                        promo = re.search(r'(\d+)?%', markers)
                        if promo is not None:
                            try:
                                promo = max([
                                    int(i) for i in promo.groups() if i is not None
                                ])
                            except:
                                promo = None
                            product['Параметр: Промо'] = promo
                        product['Параметр: Метки'] = markers
                case 2:
                    if section.find('p', attrs={'data-testid': 'price'}):
                        purchase_price = float(
                            re.sub(r'[^,\.0-9]', '', section.find('p', attrs={'data-testid': 'price'}).text).replace(',', '.')
                        )
                        if '%' in section.find('p', attrs={'data-testid': 'price'}).find_next().text:
                            sale = int(
                                re.sub('\D', '', section.find('p', attrs={'data-testid': 'price'}).find_next().text))
                            promo = product['Параметр: Промо']
                            if promo is not None:
                                purchase_price = purchase_price * (1 - promo / 100)
                            old_price = eval(
                                old_price_pattern % {'sale': sale, 'purchase_price': purchase_price}
                            )

                            product['Старая цена'] = str(round(old_price, 2)).replace('.', ',')
                            product['Параметр: Размер скидки'] = sale
                            product['Себестоимость'] = purchase_price
                case 3:
                    description = section.find('section', attrs={'data-testid': 'descriptionBlock'})
                    if description:
                        description = re.sub(r'\xa0', ' ', description.div.text.strip())
                    else:
                        description = None
                    product['Полное описание'] = description
                    characteristic = section.find('section', attrs={'data-testid': 'characteristicBlock'})
                    if characteristic:
                        table = characteristic.table
                        for it in table.find_all('tr'):
                            match it.th.text.strip().lower():
                                case 'артикул':
                                    code = it.td.text.strip()
                                case 'код товара':
                                    article = it.td.text.strip()
                                    product['Артикул'] = 'BND' + article
                                    product['Параметр: Код товара'] = article
                                case 'страна производства':
                                    country_manufacturer = it.td.text.strip()
                                    product['Параметр: Страна-производитель'] = country_manufacturer
                                case 'вес упаковки, кг':
                                    mass = float(it.td.text.strip())
                                    product['Вес, кг'] = mass
                                case 'тип':
                                    _type = it.td.text.strip()
                                    product['Параметр: Тип'] = _type.capitalize()
                                case _:
                                    pass
                    else:
                        pass

        brand = soup.find(attrs={'data-testid' : 'moreProductsItem'}).a.text.strip()
        product['Параметр: Бренд'] = brand
        product['Параметр: Производитель'] = brand
        formula = kwargs.pop('formula')
        constraints = kwargs.pop('constraints')
        if constraints:
            if brand.casefold() in constraints.keys():
                special_formula = constraints[brand.casefold()]
                if special_formula.casefold() == 'stop':
                    return
                formula = special_formula

            if product['Артикул'].casefold() in constraints.keys():
                special_formula = constraints[product['Артикул'].casefold()]
                if special_formula.casefold() == 'stop':
                    return
                if '=' in special_formula:
                    formula = special_formula
                else:
                    product['Цена продажи'] = special_formula
        formula = formula.replace('(маржа)', '').replace(',', '.').replace('р', '').replace('ЦЗ', '%(purchase_price)f').replace('вес', '%(mass)f')
        title = soup.find('h1', attrs={'data-testid' : 'pageTitle'}).text.strip()
        product['Название товара или услуги'] = title
        if (product['Себестоимость'] and product['Вес, кг']) is not None:
            if product['Цена продажи'] is None:
                sale_price = round(eval(
                    formula % {'purchase_price' : product['Себестоимость'], 'mass' : product['Вес, кг']}
                ), 2)
                product['Цена продажи'] = str(sale_price).replace('.', ',')
        
        if product['Себестоимость'] is not None:
            product['Себестоимость'] = str(round(product['Себестоимость'], 2)).replace('.', ',')
            product['Остаток'] = 100
        if product['Вес, кг'] is not None:
            product['Вес, кг'] = str(product['Вес, кг']).replace('.', ',')

        return product
        
    def handler(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')
        kwargs.pop("init")
        if soup.find('div', attrs={'data-testid' : 'variantsBlock'}):
            if 'zoozavr' in response.url:
                variants = [('https://www.zoozavr.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            else:
                variants = [('https://www.detmir.ru' + i['href'], i.text.strip()) for i in soup.find('div', attrs={'data-testid' : 'variantsBlock'}).find_all('a', attrs={'data-testid' : 'variantsItem'})]
            for url, var in variants:
                if url != response.url:
                    kwargs["variant"] = var
                    yield scrapy.Request(url, callback=self.ReceiveInfo, cb_kwargs=kwargs)
                else:
                    kwargs["variant"] = var
                    yield self.ReceiveInfo(response=response, **kwargs)
        else:
            yield self.ReceiveInfo(response=response, **kwargs)
            
    def parse(self, response, **kwargs):
        soup = BeautifulSoup(response.text, 'lxml')  
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
        
       
        if kwargs["init"] is None:
            pagination_tag = soup.find('nav', attrs={'aria-label': "pagination"})
            kwargs["init"] = True
            if pagination_tag is not None:
                max_page = int(pagination_tag.find_all("li")[-1].text)
                for idx in range(2, max_page + 1):
                    yield scrapy.Request(response.url + '?page={}'.format(idx), callback=self.parse, cb_kwargs=kwargs)   

          
