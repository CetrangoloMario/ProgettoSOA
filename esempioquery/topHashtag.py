import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import month

spark = SparkSession.builder.appName("esempio").getOrCreate()

pathcsv=r"C:\Users\cetra\Desktop\dataset\Summary_Hashtag.csv" #sys.argv[1]
#pathOutput =sys.argv[2]
print(pathcsv)
#print(pathOutput)

dfTopHashtag= spark.read.option("inferSchema","true").option("header","true").csv(pathcsv)
super=dfTopHashtag.groupby("Hashtag").count().orderBy("count",ascending=False).show()
#super.write.csv("/user/soa/dataset/ciao.csv", header=False)

print("OK")