# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 14:26:11 2021

@author: antonio.lopez
"""

import os
import pandas as pd

currentDir = './spiders/'

def arreglar_reacondicionados():
    """
    En caso de que un documento de pccomponentes no tenga la etiqueta reacondicionado, la añade y modifica el id_item
    """
    
    dataFiles = [x for x in os.listdir(currentDir + 'pccomponentes/')] #if ((os.path.isdir(currentDir + x)) and (x != '__pycache__') and x!= 'corruptos' and x!= 'wipoid')]
    #print(dataFiles)

    for file in dataFiles:
        #Carga archivos de /pccomponentes
        print('Arreglando ', file)
        f = currentDir + 'pccomponentes/' + file
        df = cargar_datos(f)
                    
        #Creamos columna reacondicionado y ponemos a False
        df['reacondicionado'] = False
        
        #Obtenemos indices de nombres que contienen la subcadena 'Refubished'
        #COmprueba nombres
        index_refurbished = df[df['nombre'].str.contains(r"\bRefurbished\b", regex=True)].index
        index_reacondicionado = df[df['nombre'].str.contains(r"\bReacondicionado\b", regex=True)].index
        #Comprueba URL en ultima instancia en caso de error
        index_reaco_url = df[df['url'].str.contains(r"\breacondicionado\b", regex=True)].index
        index_refurbished = index_reacondicionado.union(index_refurbished).union(index_reaco_url)
        print(index_refurbished)
        
        
        #Para cada indice, ponemos a True la columna reacondicionado y añadimos _Reacondicionado al id
        for i in index_refurbished:
            if df['id_item'][i].find("_reacondicionado") != -1: #Si no ha sido ya modificado antes
                df.loc[i, 'reacondicionado'] = True 
                df.loc[i, 'id_item'] = df['id_item'][i] + '_reacondicionado'
            
            
        print('columna aniadida y modificada\nSobreescribiendo ', f)
        
        df.to_json(f, orient='records', lines=True)
        

def cargar_datos(url):
    try:
        df = pd.read_json(url)
        
    except:    
        df = pd.read_json(url, lines=True)
        pass
    
    return df
    

