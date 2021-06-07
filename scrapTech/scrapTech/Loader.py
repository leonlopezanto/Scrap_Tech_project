# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 19:00:58 2021

@author: antonio.lopez
@description: Este documento contiene la clase Loader para realizar cargas de archivos
"""
import os
import shutil
from datetime import datetime
import shutil #Mueve archivos

import pandas as pd

class Loader():
    
    def __init__(self, currentDir = './'):
    
        #Direccion hasta las carpetas con los json
        self.currentDir = currentDir
        
        #Tamaño minimo que debe tener un archivo para considerarse valido
        self.minSize = 400 *1024 #En Bytes 100 KB = 102400 Bytes
        self.minSizeWi = 300*1024 # KB * 1024 B = Bytes
    
    def cargar_datos(self, url):
        """ 
        En caso de error al cargar una url lee teniendo en cuenta el parametro lines
        """

        try:
            df = pd.read_json(url)
            
        except:    
            df = pd.read_json(url, lines=True)
            pass
        
        return df
    
    
    
    def guardar_datos(self, df, path='./'):
        """
        Guarda los datos en una manera sencilla y leible, con saltos de linea
        """
        df.to_json(path, orient='records', lines=True)
    
    
    
    def extraerUltimos(self, directorio, nameDir, numFiles):
        """
        Devuelve los dos documentos más recientes
        parsea los nombres a fechas, ordena de más reciente a menos
        extrae los dos primeros si son mayores que self.minSize
        
        dt_string = "12/11/2018 09:15:32"
        "%d/%m/%Y %H:%M:%S")
        
        params:
            directorio: directorio donde se encuentran los archivos
            nombreDir: para ayudar a extraer segun la web
            
        return:
            Una lista con el numero de archivos que se han pedido
        """
        
        #Limpiamos los nombres
        extraerFinal = '_'+nameDir+'.json'
        dates = [x.replace(extraerFinal, '') for x in directorio]
        
        #Convertimos a fecha y ordenamos
        dates.sort(key=lambda date: datetime.strptime(date, "%H-%M_%d-%m"), reverse=True)
        
        #Cogemos los dos primeros siempre, y si uno está vacio se coge el siguiente 
        l=[] 
        i = 0
        eliminado = False
        while len(l) < numFiles:
            file = self.currentDir + nameDir + '/' + dates[i] + extraerFinal
            #print( file ) #'./spiders/' + nameDir + '/' + dates[i] + extraerFinal)
            #print(os.stat(file))
            if(os.stat(file).st_size < self.minSize): #Stat devuelve en bytes
                corruptDir = self.currentDir + 'corruptos/' + dates[i] + extraerFinal
                print("Trasladando " , file , " hasta " , corruptDir)
                shutil.move(file, corruptDir)
                dates.pop(i)
                eliminado = True
            else:
                l.append(file)
                
            i+=1
            
        #print("funcion interna: ", l)
            
    
        return l, eliminado
    
    def load_last_dataset(self, numFiles):
        """
        Carga los archivos y los devuelve
    
        """
        
        dataDirs = [x for x in os.listdir(self.currentDir) 
                    if ((os.path.isdir(self.currentDir + x)) and (x != '__pycache__') 
                        and (x!= 'corruptos') and (x != 'spiders') 
                        and (x != 'procesados')
                        and (x != '.ipynb_checkpoints'))]
        #print(dataDirs)
        
        urls = []
        for directorio in dataDirs: 
    #         print('Buscando elementos de ' + directorio)
    #         print(os.listdir(self.currentDir + directorio))
    #         print(dataDirs)
            l, eliminado = self.extraerUltimos(os.listdir(self.currentDir+directorio), directorio, numFiles)
            urls.append(l)
            #print('Dos últimos ficheros obtenidos: ' , nuevo, anterior)
    
            #print("funcion externa: ", urls)
        
        return urls, eliminado
    
    def getNombreArchivo(self, path, extension=False):
        """ Extrae el nombre de un archivo a partir de un path. Si extension = True devuelve la extension"""
        print(path)
        
        # si la cadena contiene ./pccomponentes/ o ./wipoid/, se extrae lo que contenga. Reemplaza con ''
        
        #Posibles rutas:
        posibles = ['./pccomponentes/', './wipoid/']
        
        
        for directorio in posibles:
            if(path.find(directorio) != -1):
                path = path.replace(directorio, '')
        
        #OPTIMIZAR
        #Si extension es False, borra la extension. Reemplaza .json con ''
        if extension == False:
            path = path.replace('.json', '') 
            
        return path
        
        
