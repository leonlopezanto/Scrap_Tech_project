# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class PcComponentesItem(scrapy.Item):
    # Items para PcComponentes 

    nombre = scrapy.Field()
    id_item = scrapy.Field()
    precio = scrapy.Field()
    fabricante = scrapy.Field() 
    categoria = scrapy.Field()
    extraccion = scrapy.Field()
    url = scrapy.Field()
    reacondicionado = scrapy.Field()
    disponible = scrapy.Field()

class WipoidItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()s
    nombre = scrapy.Field()
    id_item = scrapy.Field()
    precio = scrapy.Field()
    # fabricante = scrapy.Field() 
    categoria = scrapy.Field()
    extraccion = scrapy.Field()
    url = scrapy.Field()
    disponible = scrapy.Field()