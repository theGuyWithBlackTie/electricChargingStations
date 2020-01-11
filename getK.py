from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.getOrCreate()

#Reading points
pointsData    = spark.read.csv("/user/s2279444/PointsResult/*")
pointsData    = pointsData.selectExpr("_c0 as Latitudes", "_c1 as Longitudes")
pointsData    = pointsData.withColumn("Latitudes", pointsData["Latitudes"].cast("double"))
pointsData    = pointsData.withColumn("Longitudes", pointsData["Longitudes"].cast("double"))


#Pre Processing the dataset
columns       = pointsData.columns
assembler     = VectorAssembler(inputCols=columns,outputCol="features")
dataset       = assembler.transform(pointsData)

# Getting K for K-Means
def getK(dataset):
	K    = []
	WSSSE = []
	for k in range (2,10):
		kmeans = KMeans().setK(k).setSeed(1)
		model = kmeans.fit(dataset)
		wssse = model.computeCost(dataset)
		'''
		#Make Predictions
		predictions = model.transform(dataset)

		#Evaluate clustering by computing Silhouette score
		evaluator = ClusteringEvaluator()

		silhouette = evaluator.evaluate(predictions)'''
		K.append(k)
		WSSSE.append(wssse)
	return K,WSSSE

K,WSSSE = getK(dataset)
print("Result is: ",K,WSSSE)
