# -*- coding: utf-8 -*-
"""
Created on Fri Apr  9 18:54:25 2021

@author: antonio.lopez


Al ejecutar este archivo se lanzan los scrapers determinados.

"""
from scrapy.utils.project import get_project_settings
from spiders.pccomponentes import PcComponentesSpider
from spiders.wipoid import WipoidSpider
from scrapy.crawler import CrawlerProcess
import sys  
import time 
from Cleaning import limpiar_todo, limpiar_ultimo
from Informes import Informe

inicio = time.time()
try:
    
    arañas = ['Wipoid','PcComponentes']
    #Se crea el informe 
    info = Informe('Scrap', arañas)
    
    print('-------------------------------------------------------------------------------------------------')
    process = CrawlerProcess()
    process.crawl(PcComponentesSpider)
    process.crawl(WipoidSpider)
    
    print('-------------------------------------------------------------------------------------------------')
    process.start() # the script will block here until all crawling jobs are finished

except:
    print("El proceso ha fallado")
    info.finalInesperado()

fin = time.time()
tiempo = (fin-inicio)/60
print('-------------------------------------------------------------------------------------------------')
print('Tiempo de scrapeo: ', tiempo ,' segundos------------------------------------------------')  # 1.5099220275878906
print('-------------------------------------------------------------------------------------------------')
info.tiempoExtraccion(tiempo)
limpiar_ultimo(info)



# try:
#     limpiar_ultimo()
#     print("Limpieza con éxito")
# except:
#     print("La limpieza ha fallado")

import os 
os._exit(00)
