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


def insertar_mysql(contra):
    # Configuración de la conexión a la base de datos
    cnx = mysql.connector.connect(
        user='usuario_escritura',
        password='Mrsmi_2024',
        host='10.0.0.12',
        database='contras'
    )

    cursor = cnx.cursor()

    # Sentencia SQL para insertar los valores en la tabla contras
    sql = "INSERT INTO contras (contra) VALUES (%s)"

    try:
        # Ejecutar la sentencia SQL con los valores pasados como argumento
        cursor.execute(sql, (contra))
        cnx.commit()
        print("Registro insertado correctamente")
    except mysql.connector.Error as err:
        print(f"Error al insertar el registro: {err}")
    finally:
        # Cerrar la conexión y el cursor
        cursor.close()
        cnx.close()

###################################################################
###################################################################

def insert_mongo(_id):
    try:
      # Conexión a la base de datos local
      cliente = pymongo.MongoClient("mongodb://10.0.100:27017")
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
                    contra = _id             
                    #insert_mongo(_id)
                    insertar_mysql(contra)
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
