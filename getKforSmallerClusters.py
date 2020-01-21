'''
This file will give the array containing K from 2-9 and WSSSE values for each respective K for CLUSTERS already formed
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.getOrCreate()

totalNumberOfClusters = 5
#Reading the Clusters
clusters = {}
for clusterNumber in range(0,totalNumberOfClusters):
	read    		= spark.read.csv("/user/s2279444/Cluster"+str(clusterNumber)+"/*")
	read			= read.selectExpr("_c0 as Latitudes", "_c1 as Longitudes")
	read			= read.withColumn("Latitudes", read["Latitudes"].cast("double"))
	read			= read.withColumn("Longitudes", read["Longitudes"].cast("double"))
	clusters[clusterNumber] =  read

#Making pre-processing of data globally
tempGlobalCluster = clusters[0]
columns           = tempGlobalCluster.columns
assembler         = VectorAssembler(inputCols=columns,outputCol="features")

def getK(dataset):
	K    = []
	WSSSE = []
	for k in range(2,10):
		kmeans = KMeans().setK(k).setSeed(1)
		model = kmeans.fit(dataset)
		wssse = model.computeCost(dataset)
		K.append(k)
		WSSSE.append(wssse)
	return [K,WSSSE]


result  = {}
for clusterNumber in range(0,totalNumberOfClusters):
        currentCluster          = clusters[clusterNumber]
        dataset                 = assembler.transform(currentCluster)
        result[clusterNumber]   = getK(dataset)

print("Result of WSSSE for each bigger cluster is: ",result)	
