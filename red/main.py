import lxml
import re

import scrapy
import scrapy.core.engine
import scrapy.http

from scrapy.settings import Settings
from scrapy.crawler import CrawlerProcess



from bs4 import BeautifulSoup

from ..config import API_KEY




def get_proxy():
    import requests
    response = requests.get(f"https://proxy6.net/api/{API_KEY}/getproxy")
    data = response.json()
    result = []
    for item in data['list'].values():
        result.append(f'http://{item["user"]}:{item["pass"]}@{item["ip"]}:{item["port"]}')
    return result

class RED(scrapy.Spider):
    name = 'red'
    custom_settings = {
        "FEEDS" : {
            'RED.jsonlines' : {
                'format' : 'jsonlines',
                'overwrite' : True,
                #'indent' : 4,
                #'ensure_ascii' : False,
                'encoding' : 'utf-8'
            }
        },
        "LOG_FILE" : 'red.log',
        "LOG_FILE_APPEND" : False,
        "DOWNLOADER_MIDDLEWARES" : {
            'middlewares.ProxyRegister' : 543
        }
    }

    def start_requests(self):
        list_proxy = get_proxy()
        self.custom_settings['CONCURRENT_REQUESTS'] = len(list_proxy) + 2
        self.custom_settings['CONCURRENT_REQUESTS_PER_DOMAIN'] = len(list_proxy) + 1
        for _ in range(len(list_proxy)):
            yield scrapy.http.JsonRequest(
                url = "https://krasniykarandash.ru/ajax/geo/set.php",
                callback = lambda response : None,
                data = {
                    'id': '35',
                    'code': '0000073738',
                    'city': 'Москва',
                    'city_full': 'Москва, Россия',
                    'region': '',
                    'country_name': 'Россия',
                    'country_id': '17',
                    'country_code': 'RU',
                },
                dont_filter = True,
            )
        yield scrapy.Request(
            url = "https://krasniykarandash.ru/sitemap_products.xml",
            #url = "https://krasniykarandash.ru/product/kholst_na_podramnike_gamma_studiya_15kh15_sm_100_khlopok_melkozernistyy.html",
            callback = self.SitemapParse,
            #callback = self.product
        )
        
    def SitemapParse(self, response):
        html = response.text
        urls = re.findall(
            pattern = r'<loc>(.*?)</loc>',
            string = html
        )
        for url in urls:
            yield scrapy.Request(
                url = url,
                callback = self.product
            )

    def Table(self, table, about):
        keys = [re.sub('\W', '', i.text) for i in table.thead.find_all('th')[:-2]]
        products = table.tbody.find_all('tr')
        result = []
        if 'Наименование' in keys:
            del about['Наименование товара']
        for product in products:
            tmp = {}
            for prop, key in zip(product.find_all('td'), keys):
                match key.casefold():
                    case "вариация":
                        tmp["Свойство: Вариант"] = "https://krasniykarandash.ru" + prop.a['href']
                    case 'цена':
                        tmp["Цена"] = re.sub(r'\D', '', prop.find(class_='price-current').text)
                        if prop.find(class_='price-old') is not None:
                            tmp['Старая цена']  = re.sub(r'\D', '', prop.find(class_='price-old').text)

                    case _ :
                        tmp[key] = prop.text.strip()


            result.append({**tmp, **about})
        return result
            


        


    def product(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        placement = '/'.join(
            [
                i.span.text.strip() for i in soup.find_all('li', class_='breadcrumbs__item')
            ][1:]
        )

        soup_markers = soup.find('div', class_='prod__labels')
        if soup_markers is not None:
            markers = ' '.join([i.text for i in soup_markers.find_all('div')])
        else:
            markers = None

        title, about = soup.find_all('div', class_='col-12 col-md-6 mb-5 float-md-end')
        title = title.h1.text

        soup_decsr = about.find(id="contentDescription")
        if soup_decsr is not None:
            descr = soup_decsr.text
        else:
            descr = None

        about = {
            'Наименование товара' : title,
            'Описание' : descr.strip() if descr is not None else None,
            'Изображения' : ' '.join(["https://krasniykarandash.ru" + i['href'] for i in soup.find_all('a', attrs={'data-fancybox' : 'images'})]),
            "Расположение на сайте": placement,
            "Метки" : markers,
            **{
                ccs.find('div').text.strip() : ccs.find('div', class_='text-end').text.strip()
                for ccs in soup.find_all('div', class_='d-flex align-items-end justify-content-between mb-3')
            },
            'Ссылка на товар' : response.url,
        }

        table = soup.find('div', class_='table-responsive')
        if table is not None:
            result = self.Table(
                table,
                about 
            )
            for item in result:
                yield item
        else:
        
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

                
            yield {
                'Цена' : price.replace('.', ',') if price is not None else 'Нет в наличии',
                'Старая цена' : old_price.replace('.', ',') if old_price is not None else None,
                **about
            }


    def catalog(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        table = soup.find(id='js-catalog-container')
        products = table.find_all('div', class_='prod__item js-prod-item')
        selected_page = table.find('a', class_='pagination__item selected')
        if selected_page is not None:
            next_page = self.url + selected_page.find_next('a')['href']
            yield scrapy.Request(
                url=next_page,
                callback=self.catalog
            )

        for product in products:
            yield scrapy.Request(
                url=self.url + product.a['href'],
                callback=self.product
            )

    def closed(self, reason):
        with open ('RED.jsonlines', 'r', encoding='utf-8') as file:
            s = file.readlines()
        import json
        result = [json.loads(i) for i in s]
        # catalogs = {
        #     i : [] for i in set(
        #         prod['Расположение на сайте'].split('/')[1] for prod in result
        #     )
        # }
        # for item in result:
        #     key = item['Расположение на сайте'].split('/')[1]
        #     catalogs[key].append(item)
        import pandas as pd
        with pd.ExcelWriter('RED.xlsx', engine='xlsxwriter', engine_kwargs={'options' : {'strings_to_urls': False}}) as writer:
            # for key, value in catalogs.items():
            #     p = pd.DataFrame(value)
            #     p.to_excel(writer, index=False, sheet_name=str(key))
            p = pd.DataFrame(result)
            p.to_excel(writer, index=False, sheet_name='RESULT')



p = CrawlerProcess(Settings())
p.crawl(RED)
p.start()