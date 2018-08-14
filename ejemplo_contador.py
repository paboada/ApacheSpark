"""SimpleApp.py"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logFile = "/home/pablo/ApacheSpark/Libro.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

texto = spark.read.text(logFile)
lineas = texto.count()

wordCounts = texto.select(explode(split(texto.value, "\s+")).alias("word")).groupBy("word").count()

print("------------------------------------------------------------")
print("--------------------RESULTADO-------------------------------")
print("------------------------------------------------------------")
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
print("Lineas dentro de texto: ", lineas)
print("Palabras dentro de texto: ", wordCounts)
print("------------------------------------------------------------")
print("------------------------------------------------------------")
print("------------------------------------------------------------")



spark.stop()
