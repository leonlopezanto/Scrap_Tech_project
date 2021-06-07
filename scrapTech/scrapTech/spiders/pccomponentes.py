# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 13:11:29 2021

@author: antonio.lopez
"""
import sys 
sys.path.append('../') 



from scrapy.spiders import CrawlSpider      #Rastrear mas de una pagina. 
from scrapy.exceptions import CloseSpider      #Parar araña
from scrapy.http.request import Request

from scrapy.crawler import CrawlerProcess

from scrapy.utils.project import get_project_settings #Obtiene la configuracion de settings.py


from six.moves.urllib import parse

#from utils import get_random_agent
from time import sleep
from datetime import datetime
import logging
from scrapy.utils.log import configure_logging 


from items import PcComponentesItem 

num_items =  float('inf')


class PcComponentesSpider(CrawlSpider):#scrapy.Spider):
    name = 'pccomponentes'                         # Nombre araña
    item_count = 0                          #items contados
    #handle_httpstatus_list = [403]
    allowed_domains = ['www.pccomponentes.com']    #Dominio accesible
    
    #Exportar a JSON
    custom_settings = {
        'FEED_URI': './pccomponentes/' + datetime.now().strftime("%H-%M_%d-%m") + "_pccomponentes" + '.json',
        'FEED_FORMAT': 'json',
        'FEED_EXPORTERS': {
            'json': 'scrapy.exporters.JsonItemExporter',
        },
        'FEED_EXPORT_ENCODING': 'utf-8',
        
        'DOWNLOAD_DELAY': 1,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        # 'CONCURRENT_REQUESTS_PER_IP': 2,
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'
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

    
    def start_requests(self):
        urls = [ 'https://www.pccomponentes.com', "https://www.pccomponentes.com/placas-base","https://www.pccomponentes.com/tarjetas-graficas",'https://www.pccomponentes.com/procesadores',
                'https://www.pccomponentes.com/discos-duros', 'https://www.pccomponentes.com/discos-duros-ssd', 'https://www.pccomponentes.com/memorias-ram',
                'https://www.pccomponentes.com/torres', 'https://www.pccomponentes.com/tarjetas-sonido','https://www.pccomponentes.com/ventiladores',
                'https://www.pccomponentes.com/fuentes-alimentacion','https://www.pccomponentes.com/cables-internos-de-pc']
        
        
        headers= {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'}

        for i in urls:

            yield Request(
                url = i,
                callback = self.parse_list_page,
                headers = headers
                # meta={"proxy": "138.68.60.8:8080"}
            )


    def parse_list_page(self, response):
        next_link = response.xpath('//*[@id="pager"]/ul/li[2]/a/@href').extract_first()
        
        if next_link == None:
            next_link = response.xpath('//*[@id="pager"]/ul/li[3]/a/@href').extract_first()
            
        if next_link:
            print("Next Link: " + next_link)
            #En caso de tener una politica estricta, añadimos cosas como modificar los http headers
            
            #Concatenamos URL
            next_link = 'https://www.pccomponentes.com' + next_link # + next_link
            print('Viajando a ----', next_link)
            yield Request(
                url=next_link,
                callback=self.parse_list_page
            )
            
        for req in self.extract_product(response):
            yield req

        
        
    def extract_product(self, response):
        links = response.xpath('//a[@class="GTM-productClick enlace-superpuesto cy-product-hover-link"]/@href').extract()
        for url in links:
            result = parse.urlparse(response.url)
            base_url = parse.urlunparse((result.scheme, result.netloc, "", "", "", ""))
            url = parse.urljoin(base_url, url)
            yield Request(
                url=url,
                callback=self.parse_product_page
            )
          
    
    def parse_product_page(self, response):
        """
        Parsea los productos de la pagina web

        Parameters
        ----------
        response : El response de la web


        Yields
        ------
        ml_item : Diccionario con los datos del item extraido

        """
        print('Extracting ' + str(response))
        ml_item = PcComponentesItem()
        
        #print(str(response.xpath('normalize-space(//h1[@class="product-name"]/text())').extract()))
        ml_item['nombre'] = response.xpath('normalize-space(//*[@id="contenedor-principal"]/div[2]/div/div[3]/div/div/div[1]/h1/strong/text())').extract_first()
        
        ml_item['id_item'], ml_item['reacondicionado'] = self.seleccion_id(response, ml_item['nombre'])
             
        ml_item['precio'] = response.xpath('normalize-space(//*[@id="pcc-recommender-cross-selling"]/div[2]/div[1]/div[1]/div/div[2]/text())').extract_first()
        #ml_item['fabricante'] = response.xpath('normalize-space(//*[@id="product_reference"]/a/text())').extract()
        ml_item['categoria'] = self.seleccion_categoria(response)
        ml_item['extraccion'] = datetime.now().strftime("%d-%b-%Y (%H:%M:%S)")
        ml_item['url'] = str(response.url)
        ml_item['disponible'] = True
        print(ml_item)
        
        # #Limite de conteo
        self.item_count += 1
        if self.item_count > num_items:
            raise CloseSpider('item_exceeded')
        yield ml_item
    
    
    #Funciones seleccion Items
    
    def seleccion_id(self, response, nombre):
        """
        Devuelve el id según la estructura de la pagina
        # SI EXISTEN REVIEWS
        # //*[@id="contenedor-principal"]/div[2]/div/div[4]/div[1]/div[2]/div[2]/span[1]
        
        # SI NO EXISTEN REVIEWS
        # //*[@id="contenedor-principal"]/div[2]/div/div[4]/div[2]/div[2]/div[2]/span[1]
        
        # SI HAY OTROS VENDEDORES
        #//*[@id="contenedor-principal"]/div[2]/div/div[4]/div[1]/div[3]/div[2]/span[1]
        
        Si el nombre es reacondicionado, añadimos -REACON al id y devolvemos TRUE para la columna Reacondicionado
        """

        
        id_item = response.xpath('normalize-space(//*[@id="contenedor-principal"]/div[2]/div/div[4]/div[1]/div[2]/div[2]/span[1]/text())').extract_first()#'//*[@id="buy_block"]/div[2]/div[3]/span/text())').extract() #utilizamos como id el modelo 
        
        if not id_item: #Si no hay comentarios, la etiqueta anterior falla. En ese caso, se comprueba una etiqueta distinta
             id_item = response.xpath('normalize-space(//*[@id="contenedor-principal"]/div[2]/div/div[4]/div[2]/div[2]/div[2]/span[1]/text())').extract_first()
        
        if not id_item: #Si sigue sin haber nada, entonces hay etiqueta Otros Vendedores
            id_item = response.xpath('normalize-space(//*[@id="contenedor-principal"]/div[2]/div/div[4]/div[1]/div[3]/div[2]/span[1]/text())').extract_first()
        
        if not id_item: #Si sigue sin haber nada, entonces es que hay una oferta
            id_item = response.xpath('normalize-space(//*[@id="contenedor-principal"]/div[2]/div/div[4]/div[3]/div[2]/div[2]/span[1]/text())').extract_first()

        if((nombre.find('Refurbished') != -1) or (nombre.find('Reacondicionado') != -1)):
            #id_item = id_item + '_Reacondicionado'
            return id_item, True
        
        return id_item, False
    
    def seleccion_categoria(self,response):
        """        
        En caso de que la categoria se obtenga vacia intenta obtenerla de otro xpath
        """
        
        categoria = response.xpath('normalize-space(//*[@id="navegacion-secundaria"]/div/div[1]/a[3]/text())').extract_first()
        if not categoria:
            categoria = response.xpath('normalize-space(//*[@id="navegacion-secundaria"]/div/div[1]/a[2])/text())').extract_first()
            
        return categoria    
    
        
    #Lanzador
    def launch_PcComponentes_Scrap(self):
        """
            Lanza la araña. 
            Al final se reinicia el Kernel para la proxima ejecucion
            
            Funcion para probar la araña.
        Returns
        -------
        None.

        """
        date_extract = 'pccomponentes/' + datetime.now().strftime("%H-%M_%d-%m") + "_pccomponentes"
        
        # process = CrawlerProcess(settings = {
        #      "FEEDS": {
        #              date_extract+".json": {"format": "json"}
        #          }
        #      })
        #print('SETTINGS: ', get_project_settings())
        process = CrawlerProcess()#get_project_settings())
        process.crawl(PcComponentesSpider)
        process.start()
        
  
        print("Sleeping ...")
        sleep(0.5)
    
        import os
        os._exit(00)

    

# # run spider
# pc = PcComponentesSpider()
# pc.launch_PcComponentes_Scrap()

# process = CrawlerProcess()
# process.crawl(PcComponentesSpider)
#process.crawl(WipoidSpider)
# process.start() # the script will block here until all crawling jobs are finished




# import os 
# os._exit(00)

