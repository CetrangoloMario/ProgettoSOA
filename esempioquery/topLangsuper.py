
import csv
import glob
import sys
from pyspark.sql import SparkSession, DataFrame

#spark-submit --master local --executor-memory 5G --num-executors 4 --executor-cores 3
 #\Users\manlio\Desktop\ScriptConcatenaCSV.py
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/Summary_Details/2020_01"
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/AllSummaryDetails1.csv

spark = SparkSession.builder.appName("esempio").getOrCreate()

pathcsv= sys.argv[1]
#pathOutput =sys.argv[2]
print(pathcsv)
#print(pathOutput)

dfTopLang= spark.read.option("inferSchema","true").option("header","true").csv(pathcsv)

super=dfTopLang.groupby("Language").count()

super.write.csv("/user/soa/dataset/ciao.csv", header=False)



print("OK")
