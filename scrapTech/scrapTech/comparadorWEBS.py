# -*- coding: utf-8 -*-
"""
Created on Mon May 24 19:00:47 2021

@author: Antonio_Lopez
"""

import pandas as pd
from Loader import Loader

load = Loader()


wipoid = './procesados/maestras/wipoid/18-57-19_24-05_maestra_wipoid.json'
pccomponentes = './procesados/maestras/pccomponentes/18-58-38_24-05_maestra_pccomponentes.json'

pc = load.cargar_datos(pccomponentes)
wi = load.cargar_datos(wipoid)


pcONwi = pc[(~pc.id_item.isin(wi.id_item))]