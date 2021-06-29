import sys

import pyspark.sql.dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import month

spark = SparkSession.builder.appName("Query_su_Tweet_Covid19").getOrCreate()


def topLang(dataframe:pyspark.sql.dataframe.DataFrame):
    super = dataframe.groupby("Language").count()
    super.write.csv("/Users/cetra/Desktop/risultati/topLang.csv", header=False)

def topHashtag(dataframe:pyspark.sql.dataframe.DataFrame):
    super = dataframe.groupby("Hashtag").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topHashtag.csv", header=False)


def topSentiment(dataframe:pyspark.sql.dataframe.DataFrame):
    super = dataframe.groupby("Sentiment_Label").count()
    super.write.csv("/Users/cetra/Desktop/risultati/topSentiment.csv", header=False)

def summaryMonth(dataframe:pyspark.sql.dataframe.DataFrame):
    super=dataframe.groupby(month("Date Created")).sum("Retweets", "Likes")
    super.write.csv("/Users/cetra/Desktop/risultati/summary_month.csv", header=True)

def joinSentimentDetails(*dataframe:pyspark.sql.dataframe.DataFrame):
    dfDetails, dfSentiment=dataframe
    super = dfSentiment.join(dfDetails, "Tweet_ID", 'inner').select("Date Created", "Sentiment_Label","Tweet_ID").limit(5)
    super.write.csv("/Users/cetra/Desktop/risultati/joinSentDet.csv", header=True)


def main():
    pathDetails= "/Users/cetra/Desktop/dataset/Summary_Details.csv" #sys.argv[1]
    pathHashtag = "/Users/cetra/Desktop/dataset/Summary_Hashtag.csv"  # sys.argv[1]
    pathSentiment = "/Users/cetra/Desktop/dataset/Summary_Sentiment.csv"  # sys.argv[1]

    dfDetails = spark.read.option("inferSchema", "true").option("header", "true").csv(pathDetails)
    dfHashtag = spark.read.option("inferSchema", "true").option("header", "true").csv(pathHashtag)
    dfSentiment = spark.read.option("inferSchema", "true").option("header", "true").csv(pathSentiment)

    topLang(dfDetails)
    topHashtag(dfHashtag)
    topSentiment(dfSentiment)
    summaryMonth(dfDetails)
    joinSentimentDetails(dfDetails,dfSentiment)






main()
print("Fine")

