import os
import random
import glob

contador_linas = 0
nombre_archivo = 0
limite_lineas = 2
lineas = []
archivos_list = []
ruta = "/mnt/local/datos/Contras/OROGINALES/weakpass_pass/partidos"
directorio_donde_buscar = "/mnt/local/datos/Contras/OROGINALES/weakpass_pass/originales/"

def generar_limite_lineas():
    limite = random.randint(9999, 10000)
    return limite

def generar_randon():
    intervalo = random.randint(10000, 10000000000000)
    return intervalo
def obtener_archivos(directorio):   
   for root, dirs, files in os.walk(directorio):
        try:
            for file in files:            
                archivos_list.append(os.path.join(root, file))
                #print(file)
        except:
            print("Error al obtener archivos")
            pass

   return archivos_list


archivos_final = obtener_archivos(directorio_donde_buscar)




for archivo in archivos_final:
    limite_lineas = generar_limite_lineas()
    try:        
        with open(archivo, encoding="latin-1") as f:
            for linea in f:
                if contador_linas < limite_lineas:
                    lineas.append(linea)
                    contador_linas += 1
                elif contador_linas == limite_lineas:
                    nombre_archivo += 1
                    with open(f"{ruta}/{nombre_archivo}.txt", "w") as f2:
                        f2.writelines(lineas)
                        lineas = []
                        contador_linas = 0
                        nombre_archivo = nombre_archivo + 1
    except Exception as e:
        print(e)
        pass 
