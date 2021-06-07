# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 10:42:50 2021

@author: Antonio_Lopez
"""
import pandas as pd
import Loader as load

maestra_pc = './procesados/maestras/pccomponentes/20-54_01-06_pccomponentes.json'
maestra_wi = './procesados/maestras/pccomponentes/20-54_01-06_wipoid.json'

l = load.Loader() 

absoluta = pd.concat([maestra_pc, maestra_wi])
