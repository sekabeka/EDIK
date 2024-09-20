from dataclasses import dataclass, field

@dataclass
class Product:
    variant: str = field(default=None)
    mass: str = field(default=None)
    root: str = field(default=None)
    subcategory1: str = field(default=None)
    subcategory2: str = field(default=None)
    article: str = field(default=None)
    title: str = field(default=None)
    breadcrumbs: str = field(default=None)
    description: str = field(default=None)
    sale_price: str = field(default=None)
    old_price: str = field(default=None)
    purchase_price: str = field(default=None)
    images: str = field(default=None) 
    remain: str = field(default=None)
    brand: str = field(default=None)
    manufacterer: str = field(default=None)
    sale: str = field(default=None)
    promo: str = field(default=None)
    marks: str = field(default=None)
    code: str = field(default=None)
    _type: str = field(default=None)
    country_manufacturer: str = field(default=None)
    formulae: str = field(default=None)

