from pyspark import SparkContext, RDD
import json
import sys



def get_stations(line):
    data = json.loads(line)
    salida = data["idunplug_station"]
    entrada = data["idplug_station"]
    user = data["user_type"]
    franja_horaria = data["unplug_hourTime"]["$date"]
    return salida, entrada, user,franja_horaria



def main(nombre_archivo):
    sc = SparkContext()

    rdd = sc.textFile("201906_Usage_Bicimad.json")
    #Filtramos los usuarios, quedándonos con los de tipo 0 y 1 y eliminamos los trayectos que
    #tienen por entrada y salida la misma estación
    rdd1 = rdd.map(get_stations).filter(lambda x: x[2]<=1).filter(lambda x: x[0]!=x[1]) 

    #Eliminamos los trayectos que tienen por entrada y salida la misma estación y 
    #guardamos en rdd2 la tupla de los trayectos ordenada,1.
    rdd2 = rdd1.map(lambda x: ((min(x[0],x[1]),max(x[0],x[1])),1)) 
    
    rdd3 = rdd2.groupByKey().map(lambda x: (x[0], len(x[1])))
    
    lista_ordenados = rdd3.sortBy(lambda x : x[1]).collect()
    lista_ordenados.reverse()
    #lista_ordenados
    
    lista_100mejores = lista_ordenados[:100]
    #lista_100mejores
    
    rdd4 = rdd1.map(lambda x: (x[3],(min(x[0],x[1]),max(x[0],x[1]))))

    mejores_trayectos = [x[0] for x in lista_100mejores]
    rdd5 = rdd4.filter(lambda x : x[1] in mejores_trayectos)
    
    rdd6 = rdd5.map(lambda x: ((x[1], x[0].split("T")[1]),1)).groupByKey().map(lambda x: (x[0] ,len(x[1])))
    
    rdd7 = rdd6.map(lambda x: (x[0][0],(x[1],x[0][1]))).groupByKey().map(lambda x: (x[0],max(list(x[1]))))
    
    listaResultados = rdd7.collect()
    print(listaResultados)
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
     
