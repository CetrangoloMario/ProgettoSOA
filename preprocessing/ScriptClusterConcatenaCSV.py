from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import glob
from pyspark.shell import SparkSession
import pyspark

#pathCartellaUnire = input("Enter path cartella unire: ")
#pathOutput= input("Output path con finale: ")
#name_output=input(" Nome file output .csv: ")


#spark-submit --master local --executor-memory 5G --num-executors 4 --executor-cores 3
 #\Users\manlio\Desktop\ScriptConcatenaCSV.py
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/Summary_Details/2020_01"
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/AllSummaryDetails1.csv

spark = SparkSession.builder.getOrCreate()
sparkContext=spark.sparkContext

pathCartellaUnire ="/Users/cetra/Desktop/csv"
#sys.argv[1]
pathOutput ="/Users/cetra/Desktop/file.csv"
#sys.argv[2]


print(pathCartellaUnire)
print(pathOutput)

all_filenames=[i for i in glob.glob(pathCartellaUnire+"/*.csv")]

dest_file=pathOutput

file1=spark.read.option("inferSchema","true").option("header","true").csv(all_filenames[0])

col=file1.columns
df=spark.sparkContext.parallelize([]).toDF(file1.schema).write.csv(dest_file,header=True)

dfout=spark.read.csv(dest_file)

for file_name in sorted(all_filenames):
    dftemp=spark.read.option("inferSchema","true").option("header","true").csv(file_name)
    dfout.union(dftemp).distinct()


dfout.write.csv(dest_file,header=True)
print("OK")