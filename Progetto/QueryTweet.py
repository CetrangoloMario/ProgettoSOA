import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, instr
import pyspark.sql.functions as F


spark = SparkSession.builder.appName("Query_su_Tweet_Covid19").getOrCreate()

listmese="Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"


def topLang(dataframe):
    super = dataframe.groupby("Language").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topLang.csv", header=False)

def topHashtag(dataframe):
    super = dataframe.groupby("Hashtag").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topHashtag.csv", header=False)

def topSentiment(dataframe):
    super = dataframe.groupby("Sentiment_Label").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topSentiment.csv", header=False)



def topLangMonth(dataframe):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).groupby("Language").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/Users/cetra/Desktop/risultati/topLangMonth_"+i+".csv", header=False)

def topSentimentMonth(*dataframe):
    dfDetails, dfSentiment = dataframe
    dfjoin = dfSentiment.join(dfDetails, "Tweet_ID", 'inner')
    for i in listmese:
        super = dfjoin.filter(instr(dfjoin["Date Created"], i)>=1).groupby("Sentiment_Label").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/Users/cetra/Desktop/risultati/topSentimentMonth_"+i+".csv", header=False)

def topHashtagMonth(*dataframe):
    dfDetails, dfHashtag = dataframe
    dfjoin = dfHashtag.join(dfDetails, "Tweet_ID", 'inner')
    for i in listmese:
        super = dfjoin.filter(instr(dfjoin["Date Created"], i)>=1).groupby("Hashtag").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/Users/cetra/Desktop/risultati/topHashtagMonth_"+i+".csv", header=False)


def avgTweetsMonth(dataframe):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).select("Tweet_ID","Retweets").agg({"Tweet_ID":"avg","Retweets":"avg"})
        super.write.csv("/Users/cetra/Desktop/risultati/avgTweetsMonth_"+i+".csv", header=False)

def totTweets_Retweets_Month(dataframe:pyspark.sql.dataframe.DataFrame):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).select("Tweet_ID","Retweets").agg(F.count("Tweet_ID"),F.sum("Retweets")).show()

        #super.write.csv("/Users/cetra/Desktop/risultati/topLangMonth_"+i+".csv", header=False)



def languageSentiment(*dataframe):
    dfDetails, dfSentiment = dataframe
    dfjoin=dfSentiment.join(dfDetails, "Tweet_ID", 'inner')

    languageNegative = dfjoin.select("Language", "Sentiment_Label").filter(" Sentiment_Label == 'negative' ").groupby("Language").count().withColumnRenamed("count", "#negative")
    languagePositive = dfjoin.select("Language", "Sentiment_Label").filter(" Sentiment_Label == 'positive' ").groupby("Language").count().withColumnRenamed("count", "#positive")
    languageNeutral = dfjoin.select("Language", "Sentiment_Label").filter(" Sentiment_Label == 'neutral' ").groupby("Language").count().withColumnRenamed("count", "#neutral")
    languageNegative.write.csv("/Users/cetra/Desktop/risultati/LanguageSentiment.csv",mode="append", header=True)
    languagePositive.write.csv("/Users/cetra/Desktop/risultati/LanguageSentiment.csv",mode="append", header=True)
    languageNeutral.write.csv("/Users/cetra/Desktop/risultati/LanguageSentiment.csv",mode="append", header=True)

def summaryMonth(dataframe):
    super=dataframe.groupby(month("Date Created")).sum("Retweets", "Likes")
    super.write.csv("/Users/cetra/Desktop/risultati/summary_month.csv", header=True)

def maxLikeSentiment(*dataframe):
    dfDetails, dfSentiment=dataframe
    super = dfSentiment.join(dfDetails, "Tweet_ID", 'inner')
    maxPositive = super.filter("Sentiment_Label == 'positive'").select(F.max("Likes")).withColumnRenamed("max(Likes)", "MaxLikePositive")  # .show()
    maxNegative = super.filter("Sentiment_Label == 'negative'").select(F.max("Likes")).withColumnRenamed("max(Likes)", "MaxLikeNegative")  # .show()
    maxNeutral = super.filter("Sentiment_Label == 'neutral'").select(F.max("Likes")).withColumnRenamed("max(Likes)", "MaxLikeNeutral")  # .show()

    maxPositive.write.csv("/Users/cetra/Desktop/risultati/maxLikeSentimentPositive.csv", header=True)
    maxNegative.write.csv("/Users/cetra/Desktop/risultati/maxLikeSentimentNegative.csv", header=True)
    maxNeutral.write.csv("/Users/cetra/Desktop/risultati/maxLikeSentimentNeturral.csv", header=True)

def maxHashtagSentiment(*dataframe:pyspark.sql.dataframe.DataFrame):
    dfHastag, dfSentiment, dfDetails=dataframe
    sj=dfHastag.join(dfSentiment,"Tweet_ID",'inner')
    dfJoin=sj.join(dfDetails,"Tweet_ID",'inner')

    for i in listmese:

        try:
            super = dfJoin.filter(instr(dfJoin["Date Created"], i) >= 1).groupby("Hashtag").count().withColumnRenamed(
                "count", i).orderBy(i, ascending=False).limit(1)
            maxHashtag = super.head()["Hashtag"]
            maxPositive = dfJoin.filter("Sentiment_Label == 'positive'").where("Hashtag == '"+maxHashtag+"'").agg(F.count("Tweet_ID")).withColumnRenamed("count(Tweet_ID)","Positive_max_Hashtag_"+i)
            maxNegative = dfJoin.filter("Sentiment_Label == 'negative'").where("Hashtag =='"+maxHashtag+"'").agg(F.count("Tweet_ID")).withColumnRenamed("count(Tweet_ID)","Negative_max_Hashtag_"+i)
            maxNeutral = dfJoin.filter("Sentiment_Label == 'neutral'").where("Hashtag == '"+maxHashtag+"'").agg(F.count("Tweet_ID")).withColumnRenamed("count(Tweet_ID)","Neutral_max_Hashtag_"+i)

            maxPositive.write.csv("/Users/cetra/Desktop/risultati/Positive_max_Hashtag_"+i+".csv", header=True)
            maxNegative.write.csv("/Users/cetra/Desktop/risultati/Negative_max_Hashtag_"+i+".csv", header=True)
            maxNeutral.write.csv("/Users/cetra/Desktop/risultati/Neutral_max_Hashtag_"+i+".csv", header=True)
        except :
            print("Mese saltato: "+i)
            continue





def main():

    pathDetails= "/Users/cetra/Desktop/dataset/SummarDetails.csv" #sys.argv[1]
    pathHashtag = "/Users/cetra/Desktop/dataset/Summary_Hashtag.csv"  # sys.argv[1]
    pathSentiment = "/Users/cetra/Desktop/dataset/Summary_Sentiment.csv"  # sys.argv[1]

    dfDetails = spark.read.option("inferSchema", "true").option("header", "true").csv(pathDetails)
    dfHashtag = spark.read.option("inferSchema", "true").option("header", "true").csv(pathHashtag)
    dfSentiment = spark.read.option("inferSchema", "true").option("header", "true").csv(pathSentiment)

    #topLang(dfDetails)
    #topLangMonth(dfDetails)
    #topHashtag(dfHashtag)
    #topHashtagMonth(dfDetails,dfHashtag)
    #topSentiment(dfSentiment)
    #topSentimentMonth(dfDetails, dfSentiment)
    #avgTweetsMonth(dfDetails)
    #totTweets_Retweets_Month(dfDetails)
    #languageSentiment(dfDetails,dfSentiment)
    #summaryMonth(dfDetails)
    #maxLikeSentiment(dfDetails,dfSentiment)
    maxHashtagSentiment(dfHashtag,dfSentiment,dfDetails)





main()
spark.stop()
print("Fine")

