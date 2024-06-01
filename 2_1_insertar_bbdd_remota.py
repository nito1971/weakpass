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


def insertar_db_mysql(contra):
    # Establish connection with the MySQL database

    
    db = mysql.connector.connect(
        host="localhost",
        port=33061,
        user="root",
        password="Alcanfor_1971",
        database="contras" )   

 
    cursor = db.cursor()

    query = "INSERT INTO contras(contra) VALUES (%s)"
    try:
        cursor.execute(query, (contra,))
        db.commit()
        #print(f"Record inserted with _id: {_id}")
        cursor.close()
        db.close()
    except mysql.connector.Error as e:
        print(f"Error inserting record: {e}")
    finally:
        cursor.close()
        db.close()
    
def calcular_tiempo_ejecucion(tiempo):
    if tiempo < 60:
       print(f"Tiempo total: {tiempo:.2f} segundos")
    else:
        tiempo_minutos = tiempo / 60
        print(f"Tiempo total: {tiempo_minutos:.2f} minutos")


def get_hash(input_string):
    """
    Se obtiene el SHA-384 hash de la cadena que se le pasa cmo arguemnto.
    Si se produce un error, la función devuelve None.
    :param input_string: La cadena a procesar.
    :return: El SHA-384 hash o None si se produce un error.

    """
    try:
        return hashlib.sha384(input_string.encode()).hexdigest()
    except:
        return None

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
                    insertar_db_mysql(contra)
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
    for i in range(30):
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
