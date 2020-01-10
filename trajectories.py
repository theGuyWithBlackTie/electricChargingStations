from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

#UserDefinedFunction
mergeLatLong = udf(lambda col1, col2: [str(col1)+","+str(col2)])
mergeVehIDTrip = udf(lambda col1, col2 : col1+"-"+col2)

#Reading one file for now
readData       = spark.read.csv("/user/s2279444/projectData/VED_171101_week.csv", header=True)

#Making points to 3 decimal digits precision
readData       = readData.withColumn("Latitude[deg]", round(readData["Latitude[deg]"],3) )
readData       = readData.withColumn("Longitude[deg]", round(readData["Longitude[deg]"],3) )

tempDataToWork = readData.select("VehId", "Trip", col("Latitude[deg]").alias("Latitude"), col("Longitude[deg]").alias("Longitude"))

#Points Data
pointsData     = tempDataToWork.select("Latitude", "Longitude")
pointsData.write.csv("PointsResult")
####

tempDataToWork = tempDataToWork.withColumn("Points", mergeLatLong( col("Latitude"), col("Longitude")))
dataToWork     = tempDataToWork.withColumn("Routes", mergeVehIDTrip( col("VehId"), col("Trip") ))

finalData      = dataToWork.select("Routes", "Points")

#Accumulating all points based on Routes
temp = finalData.rdd
temp = temp.map(lambda row: (row[0],row[1]))
temp = temp.reduceByKey(lambda x,y: x+y)
temp = temp.toDF()

finalData = temp.select(col("_1").alias("Routes"), col("_2").alias("Points"))
finalData.write.csv("trajectoriesResult")
