# -*- coding: utf-8 -*-
"""
Created on Wed May 12 15:29:38 2021

@author: Antonio_Lopez
@description: Se busca 
"""
import os.path as path
from datetime import datetime

class Informe():
    
    def __init__(self, tipoInforme='Scrap', spiders=[], web='', name=''):
        
        #Dos tipos de informe: Scraping y Maestras
        self.tipoInforme = tipoInforme
        if tipoInforme == 'Scrap':
            self.dirInforme = './procesados/infoScraps/'
            self.file = "Scrap_" + datetime.now().strftime("%H-%M_%d-%m") + '.txt'
            self.spiders = spiders
        else:
            self.dirInforme = './procesados/infoMaestras/' + web + '/'
            if not name: 
                self.file = "Maestra_" + datetime.now().strftime("%H-%M-%S_%d-%m") + '_' + web + '.txt'
            else: 
                self.file = "Maestra_" + name + '_' + web + '.txt'
            
        self.ruta = self.dirInforme + self.file

            
        #Se crea archivo (Se abre (y crea) y se cierra) y aniade cabecera
        f = self.abrir()
        f.close() 
        self.cabecera()
        self.escribirInfo('WEB: ' + web)
        
    def abrir(self):
        #Comprueba si existe
        if path.exists(self.ruta):
            f = open(self.ruta, 'a')
        else:
            f = open(self.ruta, 'w+')
        return f
    

        
    # def crearArchivo(self):
    #     """
    #     El nombre del archivo es Scrap_+fecha+.txt
    #     """
        
    #     #Comprueba si existe
    #     if path.exists(self.ruta):
    #         f = open(self.ruta, 'a')
    #     else:
    #         f = open(self.ruta, 'w+')
    #         f.write("  ")
    #     f.close()
        
    def cabecera(self):
        """
        Inserta datos de cabecera de archivo:
            fecha de extraccion
            spiders lanzadas
        """
        #Comprueba si existe
        f = self.abrir()
        
        
        f.write("********************************************************************************************")
        if self.tipoInforme == 'Scrap':
            f.write('\nFecha de extraccion: ' + self.file)
            f.write('\nAraña:\n')
            for i in self.spiders:
                f.write('\t' + i + '\n')
        else:
            f.write('\nFecha de creacion: ' + self.file)
            f.write("\n********************************************************************************************")
            
        f.close()
    
    def tiempoExtraccion(self, tiempo):
        
        f = self.abrir()
        
        f.write('Tiempo de extraccion: ' + str(tiempo))
        f.write("\n********************************************************************************************")
        
        f.close
    
    def escribirInfo(self, texto):
            
        #Comprueba si existe
        f= self.abrir()
        
        f.write('\n'+texto)
        
        f.close()
    
    def finalInesperado(self):
        
        f = self.abrir()
        f.write("\n\n\nHA FALLADO EL SCRAP!")
        
        f.close()

        
    
    
    # def datosScrap(fecha, tiempoEjecucion, succesful=True):
    #     """Guarda los datos iniciales de un scrap"""
    #     directorio = './procesados/infoScraps/Scrap_' + fecha + '.txt'
        
    #     #Comprueba si existe
    #     if path.exists(directorio):
    #         f = open(directorio, 'a')
    #     else:
    #         f = open(directorio, 'w')
        
    #     #Escribe info
    #     f.write('\nFecha de extraccion: ' + fecha)
    #     f.write('\nTiempo de ejecucion: ' + str(tiempoEjecucion))
    #     f.write('\nFinalizado correctamente: '+str(succesful))
        
    #     f.close()

