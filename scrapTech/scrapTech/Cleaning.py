#!/usr/bin/env python
# coding: utf-8

# # Limpieza y guardado de información útil
# 
# #### En este documento se pretende limpiar cada dataset (eliminar caracteres especiales o archivos corruptos, normalizar letras, 
# 
# #### Tambien se intenta proporcionar un método de acceso a las tablas



import pandas as pd
import numpy as np
import os 
import os.path
from datetime import datetime
import shutil #Mueve archivos

import re
from datetime import datetime

import os.path
from os import path

#Propias
from Loader import Loader
from Informes import Informe
# ### Revisamos problemas en columnas

# #### Eliminamos el simbolo de la divisa de la columna precio


currentDir = './'
load = Loader()

def eliminar_divisa(df, nombreColumna):
    #OPERATIVO
    """
    Elimina la divisa de una columna y convierte a tipo float.
    
    Params: dataframe, nombreColumna
        -dataframe: df a modificar
        -nombreColumna: nombre de la columna a modificar
    
    """
    
    #Si hay algun precio vacio, sustituimos por -1
    #df[nombreColumna] = df[nombreColumna].fillna('-1')
    
    if(df[nombreColumna].dtype  != 'float64'):    
        df[nombreColumna] = df[nombreColumna].map(lambda x: x.replace("€", "").replace(",", "."))
        df[nombreColumna] = df[nombreColumna].astype('float64')
        # print(df[nombreColumna][0], type(df[nombreColumna][0]))
        return df
    else: 
        print("La columna ya es de tipo float64")
        return df



#valores Null
def comprobarNulos(df, file):
    """
    Comprueba si hay valores nulos y nos informa de su existencia
    """
    #Ponemos como nulos los valores que esten vacios en las columnas 
    df = df.replace('', np.nan)
    nulos = df.isnull().values.ravel().sum()
    nonulos = df.notnull().values.ravel().sum()
    print("Valores Nulos: ", nulos) #Number of null values (don't exist)
    
    # print("Valores No Nulos: ", nonulos)
    #value_when_true if condition else value_when_false
    #print("Todos los campos están completos") if nonulos == (df.shape[0] * df.shape[1]) else print("Existen ", nulos, "valores incompletos")
    
    if nonulos == (df.shape[0] * df.shape[1]): 
        print("Todos los campos están completos")
        infoNulos = "No hay campos nulos"
    else:
        print("Existen ", nulos, "valores incompletos")
        #(pd.set_option("display.max_rows", 10, "display.max_columns", 10)
        nulosDF = df[df.isnull().values]
        infoNulos = "\nLos campos nulos son" + str(nulosDF.index)
        
        rutaSave = './procesados/registrosNulos/'
        load.guardar_datos(nulosDF, rutaSave + load.getNombreArchivo(file, False) + '_nulos.json')
        print(infoNulos)
    
        #Necesario eliminar los nulos para que no nos fastidien el procesamiento posterior
    df = df.dropna()
    
    return df, infoNulos
    # pd.isnull(data["body"]).values.ravel().sum() #Number of null values (don't exist)




def arreglar_reacondicionado(df):
    """ 
    Arregla y comprueba que todos los dataframes esten correctos en cuanto a reacondicionados.
    """
    
    numReacondicionados = 0
# #         df["reacondicionado"] = True if df['nombre'].str.find(sub) 
#     #df[df['nombre'].str.contains(r"\bRefurbished\b",
#     if (df[df['nombre'].str.contains(r"\bRefurbished\b", regex=True)]):
#         df['reacondicionado'] = False
#     else:
#         df['reacondicionado'] = True

    #Creamos columna reacondicionado y ponemos a False
    df['reacondicionado'] = False

    #Obtenemos indices de nombres que contienen la subcadena 'Refubished'
    index_refurbished = df[df['nombre'].str.contains(r"\bRefurbished\b", regex=True)].index
    index_reacondicionado = df[df['nombre'].str.contains(r"\bReacondicionado\b", regex=True)].index
    
    index_refurbished = index_reacondicionado.union(index_refurbished)
    print(index_refurbished)

    #Para cada indice, ponemos a True la columna reacondicionado y añadimos _Reacondicionado al id
    for i in index_refurbished:
        if df['id_item'][i].find("_reacondicionado")== -1 or df['id_item'][i].find("_Reacondicionado")== -1: #Si no ha sido ya modificado antes
            df.loc[i, 'reacondicionado'] = True 
            df.loc[i, 'id_item'] = df['id_item'][i] + '_reacondicionado'
            numReacondicionados += 1

    print('Reacondicionados: ', numReacondicionados)
    # print('columna aniadida y modificada\n ')

    return df, numReacondicionados

def eliminar_espacios_columnas(df, listaColumnas):
    """
    Elimina espacios de las columnas en la lista

    Parameters
    ----------
    df : datafram
    listaColumnas : list()

    Returns df

    """
    for i in listaColumnas : 
        if(df[i].dtype  == 'O'):
         df[i] = df[i].str.replace(" ","")
     
    
    return df
    

def limpieza_dataset(file, informe, guardar=True, devolver=True):
    """
    Limpia un dataset y lo prepara para su procesamiento
        -Arregla problemas de reacondicionamiento si el archivo es de pccomponentes
        -Elimina las divisas de los dataframes
        -Comprueba si hay valores nulos
        -los vuelve a almacenar
    
    """
    #Carga archivos de /pccomponentes
    informe.escribirInfo("\n\n\n------------------------------------Datos de limpieza de :" + file + "\n")
    print('Arreglando ', file)
    
    load = Loader()
    df = load.cargar_datos(file)
    
    #Secuencia de pasos
    #Problemas reacondicionados si pccomponentes
    
    if(file.find('pccomponentes') != -1): 
        df, numReacondicionados = arreglar_reacondicionado(df)
        #Si hemos cogido un archivo que ya estaba limpio, así eliminamos todo lo que podamos sin dejar mierdas
        #OPTIMIZAR/REVISAR
        df, numReacondicionados = limpiar_reacondicionados(df)
        informe.escribirInfo("\nNumero de reacondicionados: " + str(numReacondicionados))
    
    #comprobar nulos
    df, infoNulos = comprobarNulos(df, file)
    

    informe.escribirInfo(infoNulos)
    
    #Elimina espacios de las columnas indicadas
    columnas_espacios = ['precio']
    df = eliminar_espacios_columnas(df, columnas_espacios)
    
    #Eliminar divisa
    eliminar_divisa(df, 'precio')
    
    #Muestra estadisticas
    print(df.describe())
    
    #En caso de duplicados en el numero de serie id_item, los muestra
    # if df[df.duplicated('id_item')]
    
    duplicados = True if(df[df.duplicated('id_item')].shape[0]  != 0) else False
    
    informe.escribirInfo("\nInformacion duplicados:\n ")
    if duplicados:
        #En caso de que, por más cojones, este duplicado, habra que alterar el numero de serie.
        #Escribe en el informe las filas que contienen duplicados
        dup = df[df.duplicated('id_item')]
        informe.escribirInfo(str(dup[['id_item', 'nombre']]))
    else:
        informe.escribirInfo("No existen duplicados")
    
    
    #INFORMACION DEL DATASET
    informe.escribirInfo("\n\nInformacion dataset:")
    #Escribir estadisticas
    informe.escribirInfo("\n\nDescripcion:\n" + str(df.describe()))
    informe.escribirInfo('\nNumero de filas: ' + str(df.shape[0]))
    informe.escribirInfo('\nNumero de columnas: ' + str(df.shape[1]))

    

    
    #Sobreescribe el archivo
    if guardar:
        load.guardar_datos(df, file)
        
    if devolver:
        return df
        



def limpiar_todo():
    """
    Limpia todos los archivos crudos que existen y los almacena de nuevo

    Returns
    -------
    None.

    """
    dataDirs = [x for x in os.listdir(currentDir) 
                if ((os.path.isdir(currentDir + x)) and (x != '__pycache__') 
                    and x!= 'corruptos' and x!= 'spiders' and x!= 'procesados') and x!='.ipynb_checkpoints']
    print(dataDirs)
    informe = Informe(['spider1'])
    for directory in dataDirs:
        dataFiles = os.listdir(currentDir + directory)
        for f in dataFiles:
            limpieza_dataset(currentDir + directory + '/' + f, informe ) 



def limpiar_ultimo(informe):
    """ 
    Limpia los archivos extraidos más recientemente
    """
    archivos, eliminado = load.load_last_dataset(1)

    if not eliminado:
    	for f in archivos:
            print("limpiando -> ", f[0] )
            limpieza_dataset(f[0], informe)
    else:   
    	informe.escribirInfo("\n\n\n--------------------- EL ARCHIVO ERA PEQUEÑO Y HA SIDO ELIMINADO ---------------------")





###############################################################################Funciones generales
#LIMPIA LA PALABRA REACONDICIONADO EN CASO DE ACUMULARSE y lo escribe si la variable es TRUE
def limpiar_reacondicionados(df): 
    
    numReacondicionados = 0
    #Obtenemos indices de nombres que contienen la subcadena 'Refubished'
    index_refurbished = df[df['nombre'].str.contains(r"\bRefurbished\b", regex=True)].index
    index_reacondicionado = df[df['nombre'].str.contains(r"\bReacondicionado\b", regex=True)].index
    
    index_refurbished = index_reacondicionado.union(index_refurbished)
    print(index_refurbished)

    #Para cada indice, ponemos a True la columna reacondicionado y añadimos _Reacondicionado al id
    for i in index_refurbished:
        if df['id_item'][i].find("_reacondicionado") != -1 or  df['id_item'][i].find("_Reacondicionado") != -1: #Si no ha sido ya modificado antes
            #df.loc[i, 'reacondicionado'] = True 
            df.loc[i, 'id_item'] = df['id_item'][i].replace('_Reacondicionado', '')#df['id_item'][i] + '_reacondicionado'
            df.loc[i, 'id_item'] = df['id_item'][i].replace('_reacondicionado', '')
            numReacondicionados += 1
    for i in index_refurbished:
        if df['reacondicionado'][i] == True:
            df.loc[i, 'id_item'] = df['id_item'][i] + '_Reacondicionado'
    
    
    return df, numReacondicionados        
    


