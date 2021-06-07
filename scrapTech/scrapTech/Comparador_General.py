# -*- coding: utf-8 -*-
"""
Created on Sun May 23 20:00:09 2021

Carga todos los datasets ordenados por fecha hasta el momento y actualiza cada uno con la tabla maestra del anterior
"""

import Loader as load
import Gestion_Maestras as Gm
import os
from datetime import datetime

#Cargamos una lista con todos los datasets de PcComponentes e iteramos sobre ellos con el objeto gestion_maestras


webs = ['wipoid', 'pccomponentes']


for w in webs:
    dataDirs = os.listdir('./' + w) 
    

    extraerFinal = '_'+w+'.json'
    dates = [x.replace(extraerFinal, '') for x in dataDirs]
    
    #Convertimos a fecha y ordenamos
    dates.sort(key=lambda date: datetime.strptime(date, "%H-%M_%d-%m"))
    
    ##gm = Gm.Gestion_maestras(w, carpeta, nombre=fecha) -> Extraemos el nombre y aniadimos el segundo
    
    
    for i in dates:
        try:
            principio = i[:5] 
            final = i[5:]
            nombre = principio + '-01' + final
            
            gm = Gm.Gestion_maestras(w, './'+ w +'/' + i + extraerFinal, nombre=nombre)
            gm.proceso()
        except:
            print("Ha fallado :"+ i)