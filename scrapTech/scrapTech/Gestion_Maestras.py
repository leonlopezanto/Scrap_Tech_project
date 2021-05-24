# -*- coding: utf-8 -*-
"""
Created on Thu May 20 20:03:06 2021

@author: Antonio_Lopez
@description: Clase para crear y gestionar tablas maestras
"""

import pandas as pd

# import sys
# sys.path.append('../../')
import Loader as load
from datetime import datetime
import numpy as np
import os
import time
from Informes import Informe

def get_fecha(tipo = ''):
    """
    Si tipo = completa -> devolverá con todo detalle el momento de la fecha 
    En caso de no poner nada, devuelve la hora ajustada a nuestro formato

    """
    return datetime.now().strftime("%d-%b-%Y (%H:%M:%S)") if(tipo == 'completa') else datetime.now().strftime("%H-%M-%S_%d-%m")


class Gestion_maestras:
    def __init__(self, web='', entrante='', maestra='', nombre=''):
      
        #Elimina la excepcion. NO nos importa
        pd.set_option('mode.chained_assignment', None)
        

        #Ruta y nombre donde guardar la tabla maestra
        self.rutaMaestras = './procesados/maestras/' + web + '/' 
        
        if not nombre:
            self.nuevoNombre = get_fecha() + '_maestra_' + web +'.json'
        else:
            self.nuevoNombre = nombre + '_maestra_' + web +'.json'
            
        self.rutaGuardado = self.rutaMaestras + self.nuevoNombre
        
        self.info = Informe('Maestra', web=web, name=nombre)
        
        #Cargamos las dos ultimas tablas
        # maestra = 'procesados/maestra_original.json' = anterior
        # nueva = 'procesados/comparador.json' = actual
        
        self.loader = load.Loader()
        
        if not maestra:
            #Cargar ultima tabla maestra
            maestra = self.obtenerUltimaMaestra(web)

        print('Cargando maestra: ' + maestra)
        self.maestra = self.loader.cargar_datos(self.rutaMaestras + maestra)
        # self.maestra.set_index('id_item')

        if not entrante:
            print('No sabe con qué comparar')
        else:
            print('Cargando entrante: ' + entrante)
            self.entrante = self.loader.cargar_datos(entrante)
            # self.entrante.set_index('id_item')

        
        #Aniadimos nombres de docs a procesar:    
        self.info.escribirInfo('Maestra: ' + maestra)
        self.info.escribirInfo('Entrante: ' + entrante)
        pd.set_option('display.max_rows', 4000000)
        
        
    def obtenerUltimaMaestra(self, web):
        """
        Carga la tabla Maestra más reciente

        """

        files = os.listdir(self.rutaMaestras)
        
        #Limpiamos el nombre
        extraerFinal = '_maestra_' + web +'.json'
        dates = [x.replace(extraerFinal, '') for x in files]
        
        #Ordenamos las tablas segun fechas en orden inverso
        dates.sort(key=lambda date: datetime.strptime(date, "%H-%M-%S_%d-%m"), reverse=True)
        
        #Devolvemos la primera y devolvemos la cadena extraida
        return dates[0] + extraerFinal
        

    def condicion_precio(self, entrante, maestra):
        conditions = [
            (entrante['precio'] > maestra['precio']),
            (entrante['precio'] < maestra['precio']),
            (entrante['precio'] == maestra['precio'])
            ]
        
        # lista de valores a asignar a las condiciones
        values = ['sube', 'baja', 'mantiene']
        
        return conditions, values
    
    def mantenidos(self):
        """
        Elementos de la tabla nueva y maestra que se mantienen (no son componentes nuevos y ni han sido eliminados).
        Puede pasar:
            1. El precio no cambia 
                - upd_precio = mantiene, update = Se mantiene del anterior, no cambia.
            2. El precio de la nueva es mayor que el precio en maestra
                -upd_precio = sube, update = Se actualiza la fecha de cambio, precio = Se actualiza con el precio de nueva
            3. El precio de la nueva es menor que el precio en maestra
                -upd_brecio = baja, update= Se actualiza la fecha de cambio, precio = Se actualiza con el precio de nueva
    
        """
        #Comprobamos los componentes de maestra que coinciden con la entrante y entrante que coinciden con maestra
        en = self.entrante[(self.entrante.id_item.isin(self.maestra.id_item))]
        maestra = self.maestra[(self.maestra.id_item.isin(self.entrante.id_item))]
        
        #ARREGLO PARA LOS INDICES:
        #Se asigna id_item como indice y se evita que se borre
        en = en.set_index('id_item', drop=False)
        maestra = maestra.set_index('id_item', drop=False)
        
        #Se ordenan los indices
        en = en.sort_index()
        maestra = maestra.sort_index()
        
        # en['index2'] = en['id_item']
        # maestra['index2'] = maestra['id_item']
        
        # en = en.set_index('index2')
        # maestra = maestra.set_index('index2')
        
        conditions, values = self.condicion_precio(en, maestra)
        
        
        #Utilizamos el metodo select de Numpy para actualizar la columna precio_upd
        maestra['precio_upd'] = np.select(conditions, values)

        #INFORME: ELEMENTOS QUE HAN CAMBIADO DE PRECIO
        self.info.escribirInfo('------------------------------------------------------------------------------------------')
        self.info.escribirInfo('Componentes que han cambiado el precio: ' + str(maestra[maestra['precio'] != en['precio']]['id_item'].count()))
        self.info.escribirInfo('Suben de precio: ' + str(maestra[maestra['precio_upd'] == 'sube'].id_item.count()))
        self.info.escribirInfo(str(maestra[maestra['precio_upd'] == 'sube'].id_item))
        self.info.escribirInfo('Bajan de precio: ' + str(maestra[maestra['precio_upd'] == 'baja'].id_item.count()))
        self.info.escribirInfo(str(maestra[maestra['precio_upd'] == 'baja'].id_item))
        self.info.escribirInfo('------------------------------------------------------------------------------------------\n')
        #Actualizamos los nuevos precios
        maestra['precio'] = en['precio']
        
        #Actualizamos la ultima actualizacion del precio
        maestra['update'].loc[(maestra['precio_upd'] == 'sube') | (maestra['precio_upd'] == 'baja')] = get_fecha('completa')
        
        #Actualizamos disponibilidad si vuelve a haber stock, actualizamos fecha y actualizamos disponibilidad
        #INFORME: ELEMENTOS QUE HAN SIDO REPUESTOS
        self.info.escribirInfo('------------------------------------------------------------------------------------------')
        self.info.escribirInfo('Elementos que vuelven a estar disponibles: ' + str(maestra[maestra['disponible'] != en['disponible']]['id_item'].count()))
        self.info.escribirInfo(str(maestra[maestra['disponible'] != en['disponible']].id_item))
        self.info.escribirInfo('------------------------------------------------------------------------------------------\n')                                   
        #Fecha de actualizacion
        maestra['update'] = np.where(maestra['disponible'] != en['disponible'], get_fecha('completa'), maestra['update'])
        
        #Actualizar disponibilidad
        maestra['disponible'] = np.where(maestra['disponible'] != en['disponible'], en['disponible'], maestra['disponible'])
        
        
        # del maestra['index']
        #maestra = maestra.reset_index()
        # del maestra['index']
        
        # en = en.reset_index()
        # del en['index']
        
        return maestra
    
    def aniadidos(self):
        
        #Elementos de entrante que no estan en maestra, componentes aniadidos
        aniadidos = self.entrante[(~self.entrante.id_item.isin(self.maestra.id_item))]
        aniadidos['precio_upd'] = 'nuevo'
        
        aniadidos['update'] = get_fecha('completa')
        
        #INFORME: Elementos aniadidos al catalogo por primera vez o despues de mucho tiempo
        self.info.escribirInfo('------------------------------------------------------------------------------------------')
        self.info.escribirInfo('Componentes nuevos: ' + str(aniadidos[aniadidos['precio_upd']=='nuevo'].id_item.count()))
        self.info.escribirInfo(str(aniadidos[aniadidos['precio_upd']=='nuevo'].id_item))
        self.info.escribirInfo('------------------------------------------------------------------------------------------\n')
        
        return aniadidos
    
    def eliminados(self):
        
        #Elementos de maestra que no aparecen en entrante, elementos fuera de stock, no disponibles
        eliminados = self.maestra[(~self.maestra.id_item.isin(self.entrante.id_item))]
        #actualizamos campo disponible
        eliminados['disponible'] = False
        
        #actualizamos campo update
        eliminados['update'] = get_fecha('completa')
        
        #Estado precio
        eliminados['precio_upd'] = 'no_disponible'
        
        #INFORME: Elementos que ya no estan disponibles para comprar
        self.info.escribirInfo('------------------------------------------------------------------------------------------')
        self.info.escribirInfo('Componentes sin stock: ' + str(eliminados[eliminados['precio_upd']=='no_disponible'].id_item.count()))
        self.info.escribirInfo(str(eliminados[eliminados['precio_upd']=='no_disponible'].id_item))
        self.info.escribirInfo('------------------------------------------------------------------------------------------\n')
        
        
        return eliminados
    
    def proceso(self):
        
        #Procesamiento de elementos mantenidos
        mantenidos = self.mantenidos()
        
        #Procesamiendo de elementos aniadidos
        nuevos = self.aniadidos()
        
        #Procesamiento de elementos eliminados
        eliminados = self.eliminados()
        
        #
        maestra = pd.concat([mantenidos, nuevos, eliminados])
        
        #Guardamos tabla maestra:
        self.guardarMaestra(maestra)
        
        
    def guardarMaestra(self, maestra):
        
        print("Guardando: " + self.rutaGuardado)
        self.loader.guardar_datos(maestra, self.rutaGuardado)
        time.sleep(0.8)
        #Desactivamos la opcion para que no escriba todo
        pd.set_option('display.max_rows', 10)
       
        
# f = ['14-01_09-04_pccomponentes', '20-23_09-04_pccomponentes', '20-40_09-04_pccomponentes']

# for i in f:
#     g = Gestion_maestras('./procesados/' + i + '.json')
#     g.proceso()

# m = 'maestra_original.json'
# e = './procesados/entrante.json'

# g = Gestion_maestras(e, m)

# g.proceso()    















