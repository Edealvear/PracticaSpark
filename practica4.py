from pyspark import SparkContext, SparkConf, RDD
from pyspark.sql import SparkSession
import sys


rdd = sc.textFile("202210.json")


def get_stations(line):
    data = json.loads(line)
    ids = data['_id']
    stations = data['stations']
    return ids, stations

rdd1 = rdd.map(get_stations)


rdd1.top(3)#Podemos ver que lo que hay son id de cada uno de los días y el historial de 


def funaux1(address):
    return (address['address'], 1)


def mapper(line):
    return list(map(funaux1,line))

rdd2 = rdd1.mapValues(mapper).flatMap(lambda x: [((x[0][0:10],x[1][i][0]),1) for i in range(len(x[1]))])
rdd2.top(20)





rdd3 = rdd2.groupByKey().map(lambda x: (x[0], len(list(x[1]))))#Aquí tenemos el número de bicis que se cogen por dia en cada estacion








rdd5prueba = rdd3.filter(lambda x: int(x[0][0][-2:])<10)
rdd5prueba.top(rdd5prueba.count())




rdd4 = rdd2.map(lambda x: (x[0][0][-2:], x[1])).groupByKey().map(lambda x: (x[0], len(list(x[1]))))#Numero de bicis cogidas por dia


#Esta es la media de bicis en octubre cogidas al día
rddMean = rdd4.map(lambda x: x[1]).mean()


minimo = rdd4.min(key = (lambda x: x[1]))
maximo = rdd4.max(key = (lambda x: x[1]))

print(f"Mínimo número de bicis que se cogen en un día:{minimo[1]}\nMáximo número de bicis que se cogen en un día :{maximo[1]}")


rdd5 = rdd4.sortBy(lambda x: int(x[0]))
rddDias = rdd5.map(lambda x: int(x[0]))
rddNumBicis = rdd5.map(lambda x: x[1])
plt.plot(rddDias.collect(), rddNumBicis.collect())
plt.title("numero de bicis por dia")
plt.ylim([5700, 6500])
plt.show()

#Como tampoco es que saquemos mucho de esto más que al parecer hay una cota en 6336


rdd5.collect()#como podemos comprobar la mayoría de los valores son 6336


#Para ello volvamos a usar rdd2
rdd6 = rdd2.groupByKey().map(lambda x: (x[0], len(list(x[1])))).\
        map(lambda x: (x[0][1],(int(x[0][0][-2:]), x[1]))).groupByKey() #agrupamos por estación
rdd6.top(3)

#Vamos a coger una muestra aleatoria de algunas estaciones distintas y vamos a dibujar cómo es el número de bicis que 
#se coge en cada una
rddSample = rdd6.sample(False, 10/rdd.count())
rddSamplelist = rddSample.collect()#Podemos ver la muestra que hemos cogido


for sample in rddSamplelist:
    sample[1].sort(key = (lambda x: x[1]))
    rddSampleDias = list(map(lambda x: x[0], sample[1]))
    rddSampleValores = list(map(lambda x: x[1], sample[1]))
    plt.plot(rddSampleDias, rddSampleValores)
    plt.title(f"Plot para estacion: {sample[0]}")
    plt.show()














