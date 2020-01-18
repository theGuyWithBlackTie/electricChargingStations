'''
This file will cluster the data. This will give the bigger clusters.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.getOrCreate()

#Reading points
pointsData    = spark.read.csv("/user/s2279444/Cluster3/*")
pointsData    = pointsData.selectExpr("_c0 as Latitudes", "_c1 as Longitudes")
pointsData    = pointsData.withColumn("Latitudes", pointsData["Latitudes"].cast("double"))
pointsData    = pointsData.withColumn("Longitudes", pointsData["Longitudes"].cast("double"))

#Pre Processing the dataset
columns       = pointsData.columns
assembler     = VectorAssembler(inputCols=columns,outputCol="features")
dataset       = assembler.transform(pointsData)

kValue        = 4
kmeans        = KMeans().setK(kValue).setSeed(1)
model         = kmeans.fit(dataset)
predictions   = model.transform(dataset)

finalData     = predictions.select("Latitudes","Longitudes", "prediction")

for i in range(0,kValue):
	data = finalData.filter(finalData["prediction"] == i)
	data = data.select(data["Latitudes"], data["Longitudes"])
	dirName = "Cluster33_"+str(i)
	print("ASHISH: Running for cluster... dirName ->",dirName)
	data.write.csv(dirName)
