import sys

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("esempio").getOrCreate()

pathcsv= sys.argv[1]
#pathOutput =sys.argv[2]
print(pathcsv)
#print(pathOutput)

dfTopLang= spark.read.option("inferSchema","true").option("header","true").csv(pathcsv)
super=dfTopLang.groupby(month("Date Created"))
super.sum("Retweets","Likes")
super.write.csv("/user/soa/dataset/ciao.csv", header=False)

print("OK")