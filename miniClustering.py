'''
This gives clustering within the cluster.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.getOrCreate()

generalKValue 	= 4
k0		= generalKValue
k1		= generalKValue
k2		= generalKValue
k3		= generalKValue
k4		= generalKValue

for i in range(0,5):
	pointsData    = spark.read.csv("/user/s2279444/Cluster"+str(i)+"/*")
	pointsData    = pointsData.selectExpr("_c0 as Latitudes", "_c1 as Longitudes")
	pointsData    = pointsData.withColumn("Latitudes", pointsData["Latitudes"].cast("double"))
	pointsData    = pointsData.withColumn("Longitudes", pointsData["Longitudes"].cast("double"))

	#Pre Processing the dataset
	columns       = pointsData.columns
	assembler     = VectorAssembler(inputCols=columns,outputCol="features")
	dataset       = assembler.transform(pointsData)

	kValue	      = generalKValue
	if(i==0):
		kValue = k0
	elif(i==1):
		kValue = k1
	elif(i==2):
		kValue = k2
	elif(i==3):
		kValue = k3
	elif(i==4):
		kValue = k4

	kmeans        = KMeans().setK(kValue).setSeed(1)
	model         = kmeans.fit(dataset)
	predictions   = model.transform(dataset)

	print("Cluster Centers of cluster",str(i))
	centers	      = model.clusterCenters()
	for center in centers:
		print(center)

	finalData     = predictions.select("Latitudes","Longitudes", "prediction")

	for j in range(0,kValue):
		data = finalData.filter(finalData["prediction"] == j)
		data = data.select(data["Latitudes"], data["Longitudes"])
		dirName = "Cluster"+str(i)+"_"+str(j)
		print("ASHISH: Running for cluster... dirName ->",dirName)
		data.write.csv(dirName)
