import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import month

spark = SparkSession.builder.appName("esempio").getOrCreate()

pathcsv=r"C:\Users\cetra\Desktop\dataset\Summary_Sentiment.csv" #sys.argv[1]
#pathOutput =sys.argv[2]
print(pathcsv)
#print(pathOutput)
path2=r"C:\Users\cetra\Desktop\dataset\Summary_Details.csv"

dfTopSentiment= spark.read.option("inferSchema", "true").option("header", "true").csv(pathcsv)
dfTopDetails=spark.read.option("inferSchema", "true").option("header", "true").csv(path2)

super=dfTopSentiment.join(dfTopDetails,"Tweet_ID",'inner').select("Date Created","Sentiment_Label","Tweet_ID").show(5)



#super.write.csv("/user/soa/dataset/ciao.csv", header=False)



print("OK")