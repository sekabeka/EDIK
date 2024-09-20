from scrapy import Spider

from src.items import Product

class ValidatePipeline:
    def process_item(self, item: Product, spider: Spider):
        item._type = item._type.capitalize()