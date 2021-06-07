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
from datetime import datetime
import Loader as load
import Gestion_Maestras as Gm
import os



inicio = time.time()
try:
    #'wipoid',
    arañas = ['wipoid', 'pccomponentes']
    #Se crea el informe 
    fecha = datetime.now().strftime("%H-%M-%S_%d-%m")
    info = Informe('Scrap', arañas)

    print('-------------------------------------------------------------------------------------------------')
    process = CrawlerProcess()
    process.crawl(PcComponentesSpider)
    # process.crawl(WipoidSpider)
    
    print('-------------------------------------------------------------------------------------------------')
    process.start() # the script will block here until all crawling jobs are finished

    fin = time.time()
    tiempo = (fin-inicio)/60
    print('-------------------------------------------------------------------------------------------------')
    print('Tiempo de scrapeo: ', tiempo ,' segundos------------------------------------------------')  # 1.5099220275878906
    print('-------------------------------------------------------------------------------------------------')
    info.tiempoExtraccion(tiempo)
    limpiar_ultimo(info)
    
    
    
    #Cargamos el ultimo dataset extraido de cada web
    
    l = load.Loader()
    
    for w in arañas:
        dataDirs = os.listdir('./' + w) 
    
        extraerFinal = '_'+w+'.json'
        dates = [x.replace(extraerFinal, '') for x in dataDirs]
        
        #Convertimos a fecha y ordenamos
        dates.sort(key=lambda date: datetime.strptime(date, "%H-%M_%d-%m"))
        carpeta = './'+ w +'/' + dates[-1] + extraerFinal
        gm = Gm.Gestion_maestras(w, carpeta, nombre=fecha)
        gm.proceso()

except:
    print("El proceso ha fallado")
    info.finalInesperado()




# try:
#     limpiar_ultimo()
#     print("Limpieza con éxito")
# except:
#     print("La limpieza ha fallado")

import os 
os._exit(00)
