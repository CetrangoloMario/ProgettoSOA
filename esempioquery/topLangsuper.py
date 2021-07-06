import datetime

from numpy import isin
from pyspark.sql import SparkSession, DataFrame

#spark-submit --master local --executor-memory 5G --num-executors 4 --executor-cores 3
 #\Users\manlio\Desktop\ScriptConcatenaCSV.py
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/Summary_Details/2020_01"
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/AllSummaryDetails1.csv
from pyspark.sql.functions import month, array_contains, instr, col

spark = SparkSession.builder.appName("esempio").getOrCreate()

pathcsv=  "/Users/cetra/Desktop/dataset/SummarDetails.csv"#sys.argv[1]
#pathOutput =sys.argv[2]
print(pathcsv)
#print(pathOutput)

mese="Jan","Feb"
dfTopLang= spark.read.option("inferSchema","true").option("header","true").csv(pathcsv)
for i in mese:
    super = dfTopLang.filter(instr(dfTopLang["Date Created"], i)>=1).groupby("Language").count().withColumnRenamed("count",i).show()


#super.write.csv("/user/soa/dataset/ciao.csv", header=False
print("OK")
