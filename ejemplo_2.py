from pyspark import SparkContext, SparkConf
from operator import add

logFile = "/home/pablo/ApacheSpark/Libro.txt"  # Should be some file on your system
conf = SparkConf().setAppName("ContadorPalabras").setMaster("local")
sc = SparkContext(conf=conf)
distFile = sc.textFile(logFile)
rdd = distFile.collect() #el RDD leido como un arreglo con collect
print(" ")
print("***********************************************************")
print("***********************************************************")
print("El archivo leido en el contexto se convierte en un RDD of Strings, en donde cada posicion del collect es una linea del texto")
print("Primeras 3 linnas del RDD leido como un arreglo a traves de collect")
print(rdd[0])
print(rdd[1])
print(rdd[2])
print(" ")
print("Dado que el RDD dividio el texto en lineas,distFile.count,cuenta las lineas:", distFile.count())
print("***********************************************************")
print("***********************************************************")
print(" ")

print("distFile.map, Return a new RDD by applying a function to each element of this RDD.")
#En este caso s es cada elemento del RDD inicial, es decir que s es cada una de las lineas del RDD inicial. Al usar map, lo que hacemos es aplicar la funcion (s,1) a cada elemento. Todos los
#elementos forman un RDD nuevo en donde cada elemento del RDD resultante es una pareja (s,1)
rdd_map = distFile.map(lambda s: (s,1)).collect()
print(rdd_map[0])
#En ese orden de ideas si uso la funcion len a cada uno de los elementos, obtendre un RDD con la longitud de cada una de las lineas
rdd_len = distFile.map(lambda s: len(s)).collect()
print(rdd_len)

#reduce: Reduces the elements of this RDD using the specified commutative and associative binary operator. Currently reduces partitions locally.
rdd_longitudes = distFile.map(lambda s: len(s))
res = rdd_longitudes.reduce(lambda a, b: a+b)
res2= rdd_longitudes.reduce(add)
print(res, res2)

#Longitud de caracteres todo el documento
#resultado2 = distFile.map(lambda s: len(s)).reduce(lambda a, b: a+b)
print("***********************************************************")
print("***********************************************************")
#print("Longitud de caracteres de la primera linea ", resultado)
#print("Longitud de caracteres de todo el documento ", resultado2)
print("***********************************************************")
print("***********************************************************")
