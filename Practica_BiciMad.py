'''
            PRÁCTICA 4 SPARK

Integrantes: 

- Enrique E. de Alvear Doñate
- Lucía Roldán Rodriguez
- Laura Cano Gómez
            
'''


from pyspark import SparkContext, RDD
import json
import sys


# Funcion para obtener los datos de BiciMad
def get_stations(line):
    data = json.loads(line)
    salida = data["idunplug_station"]
    entrada = data["idplug_station"]
    user = data["user_type"]
    franja_horaria = data["unplug_hourTime"]["$date"]
    edad = data["ageRange"]
    return salida, entrada, user,franja_horaria, edad

def get_year(sc):#Para ejecutar en el cluster
    rdd = sc.textFile("/public_data/bicimad/201901_movements.json")
    for i in range(2, 10):
        rdd.union(sc.textFile(f"/public_data/bicimad/20190{i}_movements.json"))#Los separo para poder hacer diferencia entre los que tienen 06, 07,08... y los de dos cifras 10,11,12...
    for i in range(10,13):
        rdd.union(sc.textFile())
    return rdd
    
def get_data(sc, lista_Archivos):
    rdd = sc.textFile(lista_Archivos.pop(0))#Nos creamos el rdd
    for textName in lista_Archivos:
        rdd.union(sc.textFile(textName))#Le vamos añadiendo todos los archivos que hayamos metido 
    return rdd
    
def main(lista_Archivos = []):
    sc = SparkContext()
    #if nombre_archivo == "TODOS":
    #    rdd = get_year(sc)
    #else:
    #    rdd = sc.textFile(nombre_archivo)
    rdd = get_data(sc, lista_Archivos)
    #Filtramos los usuarios, quedándonos con los de tipo 0 y 1
    rdd1 = rdd.map(get_stations).filter(lambda x: x[2]<=1).filter(lambda x: x[0]!=x[1]) 

    #Eliminamos los trayectos que tienen por entrada y salida la misma estación y 
    #guardamos en rdd2 la tupla de los trayectos ordenada.
    rdd2 = rdd1.map(lambda x: ((min(x[0],x[1]),max(x[0],x[1])),1)) 
    

    # Agrupamos los trayectos y obtenemos el numero de veces que se realiza cada uno
    rdd3 = rdd2.groupByKey().map(lambda x: (x[0], len(x[1])))
    maximo = rdd3.max(key = (lambda x: x[1]))[0]


    # Ordenamos los trayectos de mayor a menor frecuencia
    lista_ordenados = rdd3.sortBy(lambda x : x[1]).collect()
    lista_ordenados.reverse()

    # Seleccionamos los 100 trayectos mas frecuentes    
    lista_100mejores = lista_ordenados[:100]

    # Por cada uno de los 100 trayectos, obtenemos la hora y el dia en el que se realizan
    rdd4 = rdd1.map(lambda x: (x[3],(min(x[0],x[1]),max(x[0],x[1]))))
    mejores_trayectos = [x[0] for x in lista_100mejores]
    rdd5 = rdd4.filter(lambda x : x[1] in mejores_trayectos)
    
    # Nos quedamos unicamente con el dato de la hora mas frecuente a la que se realiza el trayecto, con ello 
    # podremos contabilizarlas mensualmente
    rdd6 = rdd5.map(lambda x: ((x[1], x[0].split("T")[1]),1)).groupByKey().map(lambda x: (x[0] ,len(x[1])))
    
    # Seleccionamos la hora a la que se realiza ese trayecto con mayor frecuencia, para poder habilitar un 
    # carril para ese trayecto mientras se construye el nuevo carril bici
    rdd7 = rdd6.map(lambda x: (x[0][0],(x[1],x[0][1]))).groupByKey().map(lambda x: (x[0],max(list(x[1]))))
    

    # Obtenemos la franja de edad que mas usa el servicio
    rddEdad = rdd1.map(lambda x: (x[4], 1)).groupByKey().map(lambda x: (int(x[0]), len(x[1])))
    sol_edad= rddEdad.max(key= lambda x: x[1])



    # Muestra de los resultados en la terminal
    listaResultados = rdd7.collect()
    print(f'\nLos trayectos y horas seleccionadas son: \n(Formato de los datos -> ((Inicio, Final), (frecuencia_de_viajes, hora_con_mas_afluencia)) \n {listaResultados}')
    print(f'\nLa franja de edad que mas usa el servicio y su numero de trayectos es: {sol_edad}')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(list(sys.argv[1:]))

     
