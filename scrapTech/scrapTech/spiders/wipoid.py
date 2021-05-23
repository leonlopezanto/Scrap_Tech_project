# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 13:11:29 2021

@author: antonio.lopez
"""


import sys 
sys.path.append('../')

#from icecream import ic 

from scrapy.spiders import CrawlSpider      #Rastrear mas de una pagina. 
from scrapy.spiders import Rule 
from scrapy.linkextractors import LinkExtractor #Especifica cómo extraer enlaces de la URL rastreada.
from scrapy.exceptions import CloseSpider      #Parar araña

from scrapy.crawler import CrawlerProcess

from time import sleep
from datetime import datetime
from scrapy.utils.project import get_project_settings #Obtiene la configuracion de settings.py

from items import WipoidItem 
import logging
from scrapy.utils.log import configure_logging 

#ic.disable()

num_items = float('inf')

class WipoidSpider(CrawlSpider):
    name = 'wipoid'                         # Nombre araña
    item_count = 0                          #items contados

    allowed_domains = ['www.wipoid.com']    #Dominio accesible
    #URL inicial del proceso.
    start_urls = ['https://wipoid.com', 'https://www.wipoid.com/tarjetas-graficas/','https://www.wipoid.com/procesadores/',
                  'https://www.wipoid.com/placas-base/','https://www.wipoid.com/memoria-ram/','https://www.wipoid.com/discos-duros/',
                  'https://www.wipoid.com/fuentes-alimentacion/', 'https://www.wipoid.com/torres-cajas-carcasas/', 'https://www.wipoid.com/ventiladores/',
                  'https://www.wipoid.com/tarjetas-de-sonido/', 'https://www.wipoid.com/capturadoras-video/', 'https://www.wipoid.com/tarjetas-de-interfaz/',
                  'https://www.wipoid.com/refrigeracion-aire/','https://www.wipoid.com/cables-y-adaptadores/'] #Pueden anadirse mas
    
    #Exportar a JSON
    custom_settings = {
        'FEED_URI': './wipoid/' + datetime.now().strftime("%H-%M_%d-%m") + "_wipoid" + '.json',
        'FEED_FORMAT': 'json',
        'FEED_EXPORTERS': {
            'json': 'scrapy.exporters.JsonItemExporter',
        },
        'FEED_EXPORT_ENCODING': 'utf-8',
        
        'DOWNLOAD_DELAY': 1,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        # 'CONCURRENT_REQUESTS_PER_IP': 2,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
        # 'DOWNLOADER_MIDDLEWARES': {
        #     'nombrespider.middlewares.NombrespiderDownloaderMiddleware': 543,
        #     'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 593,  
        # }

    }
    
    configure_logging(install_root_handler=False)
    logging.basicConfig(
        filename='log.txt',
        format='%(levelname)s: %(message)s',
        level=logging.INFO
    )
    
    #Reglas
    rules = {
        #cada xpath es distinto, estructuracion del HTML
        #Esto es para que en la scrollbar del numero de paginas disponibles de elementos, cambie al siguiente
        # 1,2,3 ... ultima -> va del 1 al 2 del 2 al 3 etc
        Rule(LinkExtractor(allow = (), restrict_xpaths = ('//li[@class="pagination_next"]/a'))) ,
        
        #Esta regla entra en cada producto para recopilar la informacion
        Rule(LinkExtractor(allow = (), restrict_xpaths = ('//a[@class="product-name"]')), callback = 'parse_item', follow = False) 

    }
    
    #print("------------------------------------------------------------------------------------COMENZANDO------------------------------------------------------------------")
     
    
    
    def parse_item(self, response):
        #ic(response)
        ml_item = WipoidItem()
        
        #print(str(response.xpath('normalize-space(//h1[@class="product-name"]/text())').extract()))
        ml_item['nombre'] = response.xpath('normalize-space(//*[@id="buy_block"]/h1/text())').extract_first()
        ml_item['id_item'] = response.xpath('normalize-space(//*[@id="buy_block"]/div[2]/div[3]/span/text())').extract_first()#'//*[@id="buy_block"]/div[2]/div[3]/span/text())').extract() #utilizamos como id el modelo 
        ml_item['precio'] = response.xpath('normalize-space(//*[@id="our_price_display"]/text())').extract_first()
        #ml_item['fabricante'] = response.xpath('normalize-space(//*[@id="product_reference"]/a/text())').extract()
        ml_item['categoria'] = response.xpath('normalize-space(//*[@id="sns_pathway"]/div/a[3])').extract_first()
        ml_item['extraccion'] = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        ml_item['url'] = str(response.url)
        
        
        ml_item['disponible'] = self.disponibilidad(response) #response.xpath('disponibilidad') = //*[@id="center_column"]/ul/li[19]/div/div/div/div[2]/div[2]/a/img/@src
        
        print(ml_item)
        self.item_count += 1
        if self.item_count > num_items:
            raise CloseSpider('item_exceeded')
        yield ml_item
    
     
    def disponibilidad(self, response):
        
        img_name = str(response.xpath('//*[@id="availability_statut"]/a/img/@src').extract_first()).find('nodisponible')
        # if img_name == -1:
        #     ic(response.url)
        return True if img_name == -1 else False

 
    def launch_Wipoid_Scrap(self):
        
        # date_extract = 'pccomponentes/' + datetime.now().strftime("%H-%M_%d-%m") + "_pccomponentes"
        
        # process = CrawlerProcess(settings = {
        #     "FEEDS": {
        #             date_extract+".json": {"format": "json"}
        #         }
        #     })
        
        process = CrawlerProcess(get_project_settings())
        process.crawl(WipoidSpider)
        process.start()
        
        # i += 1
        print("Sleeping ...")
        sleep(0.5)
    
        import os
        os._exit(00)

# run spider
# w = WipoidSpider()
# w.launch_Wipoid_Scrap()