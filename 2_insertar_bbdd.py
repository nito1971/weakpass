import threading
import os
import random
import pymongo
import time
import pathlib
import mysql.connector
from mysql.connector import Error

ruta = "/mnt/local/datos/Contras/OROGINALES/weakpass_pass/partidos"

def numero_archivos():
    initial_count = 0
    for path in pathlib.Path(ruta).iterdir():
        if path.is_file():
            initial_count += 1

    return(initial_count)
###################################################################
###################################################################

###################################################################
###################################################################

def insertar_mysql(usuario, passwd_text):    
    # Conexión a la base de datos MySQL
    try:
        conexion = mysql.connector.connect(
            host='localhost',
            user='root',
            passwd='Alcanfor_1971',
            database='contras'
        )

        cursor = conexion.cursor()

        # Crear la consulta SQL para insertar datos
        consulta = f"INSERT INTO rockyou(usuario, passwd_text) VALUES (%s, %s)"

        # Ejecutar la consulta SQL
        try:
            cursor.execute(consulta, (usuario, passwd_text))
            conexion.commit()
            #print(f"Los datos correspondientes han sido insertados correctamente.")
            conexion.close()
        except Error as e:
            #print(f"Ocurrió un error al insertar los datos: {e}")
            conexion.rollback()  # Revierte en caso de error
    except Error as e:
        #print(f"Ocurrió un error al conectar a MySQL: {e}")
        pass

###################################################################
###################################################################

def insert_mongo(_id):
    try:
      # Conexión a la base de datos local
      cliente = pymongo.MongoClient("mongodb://localhost:27019")
      # Seleccionar la base de datos y la colección
      db = cliente["contras"]
      coleccion = db["contras"]
      # Datos a insertar
      datos = [
        {
          "_id": _id
                 
        }      
      ]

      # Insertar los datos en la colección
      coleccion.insert_many(datos)

      # Cerrar la conexión a la base de datos
      cliente.close()
    except Exception as e:
       print(f"Error al insertar en MongoDB: {e}")
       cliente.close()
       return False

###################################################################
###################################################################
    
def inicio(hilo):
    print(f"Hilo {hilo} iniciado.")
    inicio = time.time()
    try:
        if((len(ruta)) > 0):        
            lista_archivos = os.listdir(ruta)
            indice_random = random.randint(0, len(lista_archivos))
            archivo = lista_archivos[indice_random]
            ruta_archivo = os.path.join(ruta, archivo)
            with open(ruta_archivo, "r") as f:
                for linea in f:
                    linea_a_hashear = linea.rstrip("\n")                    
                    _id = linea_a_hashear                   
                    datos = linea_a_hashear              
                    insert_mongo(_id)
                    #print(f"Usuario: {usuario} - Passwd: {passwd}")
                               
                os.remove(ruta_archivo)                
                print(f"{archivo} ha sido eliminado.")
                final = time.time()
                tiempo_total = (final - inicio)
                print(f"Tiempo total de ejecución: {tiempo_total} segundos para el hilo {hilo}")
                print(f"Quedan {numero_archivos()} archivos aún")
    except Exception as e:
        print(e)
        return False     

###################################################################
###################################################################

while(numero_archivos() > 0):
   # Creamos una lista de 5 hilos.
    os.system("clear")

    hilos = []
    print("#" *30)
    print(f"Quedan {numero_archivos()} archivos aún")
    print("#" *30)
    for i in range(10):
        hilo = threading.Thread(target=inicio, args=(i,))
        hilos.append(hilo)

    # Iniciamos todos los hilos.
    for hilo in hilos:
        hilo.start()
    
        # Esperamos a que todos los hilos terminen.
    for hilo in hilos:
        hilo.join()

    



    
    if(numero_archivos() == 0):
        break

   
print("Terminado")
