import glob
from functools import reduce

from pyspark.sql import SparkSession, DataFrame

#spark-submit --master local --executor-memory 5G --num-executors 4 --executor-cores 3
 #\Users\manlio\Desktop\ScriptConcatenaCSV.py
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/Summary_Details/2020_01"
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/AllSummaryDetails1.csv



spark = SparkSession.builder.appName("esempio").getOrCreate()

def main():
    pathCartellaUnire = r"C:\Users\cetra\Desktop\dataset\Summary_Details\2020_01"
    #sys.argv[1]
    pathOutput  = r"C:\Users\cetra\Desktop\dataset\output.csv"
    #sys.argv[2]
    print(pathCartellaUnire)
    print(pathOutput)
    concatena(pathCartellaUnire,pathOutput)
    print("ok")


def concatena(pathCartellaUnire,dest_file):
    all_filenames = [i for i in glob.glob(pathCartellaUnire + "/*.csv")]
    file1 = spark.read.option("inferSchema", "true").option("header", "true").csv(all_filenames[0])

    dfout = spark.sparkContext.parallelize([]).toDF(file1.schema)#.write.csv(dest_file, header=True)
    dfout = spark.read.csv(dest_file)
    #dfs=[]
    for file_name in sorted(all_filenames):
        dftemp = spark.read.option("inferSchema", "true").option("header", "true").csv(file_name)
        dfout.union(dftemp)
        #dftemp.show()
        #dfout.union(dftemp).distinct()

    dfout=reduce(DataFrame.unionAll, dfout)

    dfout.write.save(path=dest_file,format="csv",sep=",")



main()


