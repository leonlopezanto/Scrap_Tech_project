# -*- coding: utf-8 -*-
"""
Created on Thu May 20 20:03:06 2021

@author: Antonio_Lopez
"""

import pandas as pd

import sys
#sys.path.append('../../')
#import Loader as load
from datetime import datetime

def get_fecha(tipo = 'normal'):
    return datetime.now().strftime("%d-%b-%Y (%H:%M:%S)") if(tipo == 'normal') else datetime.now().strftime("%H-%M_%d-%m")



#eliminados.is_copy = None
#Elimina la excepcion. NO nos importa
pd.set_option('mode.chained_assignment', None)





#Cargamos las dos ultimas tablas
actual = './actual_peq.json'
ant = './anterior_peq.json'

#Carga en nuestra app
#l = load.Loader()
# #Cargador de datasets 
# actual = Loader.cargar_datos(actual)

actual = pd.read_json(actual, lines=True)
ant = pd.read_json(ant, lines=True)

print("Elementos en actual: " , actual.count())
print("Elementos en ant: ", ant.count())

an = ant[(ant.id_item.isin(actual.id_item))].reset_index()
ac = actual[(actual.id_item.isin(ant.id_item))].reset_index()

import numpy as np
# Lista de condiciones
conditions = [
    (an['precio'] < ac['precio']),
    (an['precio'] > ac['precio']),
    (an['precio'] == ac['precio'])
    ]
# lista de valores a asignar a las condiciones
values = ['sube', 'baja', 'mantiene']

#Utilizamos el metodo select de Numpy
ac['precio_upd'] = np.select(conditions, values)


#ac.loc['update'] = ac['precio_upd'].apply(lambda x: date if x != 'mantiene' else ac.loc['update'])
ac['update'].loc[(ac['precio_upd'] == 'sube') | (ac['precio_upd'] == 'baja')] = get_fecha()
del ac['index']

eliminados = ant[(~ant.id_item.isin(actual.id_item))]

print('Mostrando elementos de anterior que no están en actual')
print(eliminados)


#actualizamos campo disponible
eliminados['disponible'] = False
#actualizamos campo update
eliminados['update'] = get_fecha()

#Unimos los archivos que no estan disponibles a la tabla maestra de pccomponentes
#maestraPC = actual.merge(eliminados, how='outer')
#variable maestraPC con elementos no disponibles y disponibles actualizados y update actualizado
#maestraPC

#Necesita aún lo del precio....

eliminados['precio_upd'] = 'mantiene'

eliminados.shape





nuevos = actual[(~actual.id_item.isin(ant.id_item))]

nuevos['precio_upd'] = 'Nuevo'

nuevos.head(30)
nuevos.shape


maestraPC = pd.concat([nuevos,ac,eliminados])



maestraPC.sort_values(by=['id_item']).reset_index()
maestraPC.shape





















