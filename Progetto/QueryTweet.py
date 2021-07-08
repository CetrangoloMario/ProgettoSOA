import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, instr
import pyspark.sql.functions as F


spark = SparkSession.builder.appName("Query_su_Tweet_Covid19").getOrCreate()

listmese="Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"
listgiorni ="Jan 22", "Mar 11","Dec 14", "Feb 21", "Oct 28"


def topAllDay(*dataframe ):
    dfDetails,dfSentiment,dfHastag  = dataframe
    sj = dfHastag.join(dfSentiment, "Tweet_ID", 'inner')
    dfJoin = sj.join(dfDetails, "Tweet_ID", 'inner')
    topAllDay1(dfJoin)
    topAllDay2(dfJoin)
    topAllDay3(dfJoin)

def topAllDay1(dfJoin):
    for i in listgiorni:
        try:
            super = dfJoin.filter(instr(dfJoin["Date Created"], i) >= 1)
            topLang(super)
        except:
           # print("Giorno saltato: " + i)
            continue

def topAllDay2(dfJoin):
    for i in listgiorni:
        try:
            super = dfJoin.filter(instr(dfJoin["Date Created"], i) >= 1)
            topSentiment(super)
        except:
           # print("Giorno saltato: " + i)
            continue

def topAllDay3(dfJoin):
    for i in listgiorni:
        try:
            super = dfJoin.filter(instr(dfJoin["Date Created"], i) >= 1)
            topHashtag(super)
        except:
           # print("Giorno saltato: " + i)
            continue




def topLang(dataframe):
    super = dataframe.groupby("Language").count().orderBy("count", ascending=False)
    super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topLang.csv",mode="append", header=False)

def topHashtag(dataframe):
    super = dataframe.groupby("Hashtag").count().orderBy("count", ascending=False)
    super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topHashtag.csv",mode="append", header=False)

def topSentiment(dataframe):
    super = dataframe.groupby("Sentiment_Label").count().orderBy("count", ascending=False)
    super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topSentiment.csv",mode="append", header=False)



def topLangMonth(dataframe):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).groupby("Language").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topLangMonth_"+i+".csv",mode="append", header=False)

def topSentimentMonth(*dataframe):
    dfDetails, dfSentiment = dataframe
    dfjoin = dfSentiment.join(dfDetails, "Tweet_ID", 'inner')
    for i in listmese:
        super = dfjoin.filter(instr(dfjoin["Date Created"], i)>=1).groupby("Sentiment_Label").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topSentimentMonth_"+i+".csv",mode="append", header=False)

def topHashtagMonth(*dataframe):
    dfDetails, dfHashtag = dataframe
    dfjoin = dfHashtag.join(dfDetails, "Tweet_ID", 'inner')
    for i in listmese:
        super = dfjoin.filter(instr(dfjoin["Date Created"], i)>=1).groupby("Hashtag").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topHashtagMonth_"+i+".csv",mode="append", header=False)


def avgTweetsMonth(dataframe):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).select("Tweet_ID","Retweets").agg({"Tweet_ID":"avg","Retweets":"avg"})
        super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/avgTweetsMonth_"+i+".csv",mode="append", header=False)

def totTweets_Retweets_Month(dataframe):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).select("Tweet_ID","Retweets").agg(F.count("Tweet_ID"),F.sum("Retweets"))
        super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/topLangMonth_"+i+".csv",mode="append", header=False)



def languageSentiment(*dataframe):
    dfDetails, dfSentiment = dataframe
    dfjoin=dfSentiment.join(dfDetails, "Tweet_ID", 'inner')

    languageNegative = dfjoin.select("Language", "Sentiment_Label").filter(" Sentiment_Label == 'negative' ").groupby("Language").count().withColumnRenamed("count", "#negative")
    languagePositive = dfjoin.select("Language", "Sentiment_Label").filter(" Sentiment_Label == 'positive' ").groupby("Language").count().withColumnRenamed("count", "#positive")
    languageNeutral = dfjoin.select("Language", "Sentiment_Label").filter(" Sentiment_Label == 'neutral' ").groupby("Language").count().withColumnRenamed("count", "#neutral")
    languageNegative.write.csv("/user/soa/cetrangolo_santonastaso/risultati/LanguageSentiment.csv",mode="append", header=True)
    languagePositive.write.csv("/user/soa/cetrangolo_santonastaso/risultati/LanguageSentiment.csv",mode="append", header=True)
    languageNeutral.write.csv("/user/soa/cetrangolo_santonastaso/risultati/LanguageSentiment.csv",mode="append", header=True)

def summaryMonth(dataframe):
    super=dataframe.groupby(month("Date Created")).sum("Retweets", "Likes")
    super.write.csv("/user/soa/cetrangolo_santonastaso/risultati/summary_month.csv", header=True)

def maxLikeSentiment(*dataframe):
    dfDetails, dfSentiment=dataframe
    super = dfSentiment.join(dfDetails, "Tweet_ID", 'inner')
    maxPositive = super.filter("Sentiment_Label == 'positive'").select(F.max("Likes")).withColumnRenamed("max(Likes)", "MaxLikePositive")  # .show()
    maxNegative = super.filter("Sentiment_Label == 'negative'").select(F.max("Likes")).withColumnRenamed("max(Likes)", "MaxLikeNegative")  # .show()
    maxNeutral = super.filter("Sentiment_Label == 'neutral'").select(F.max("Likes")).withColumnRenamed("max(Likes)", "MaxLikeNeutral")  # .show()

    maxPositive.write.csv("/user/soa/cetrangolo_santonastaso/risultati/maxLikeSentimentPositive.csv",mode="append", header=True)
    maxNegative.write.csv("/user/soa/cetrangolo_santonastaso/risultati/maxLikeSentimentNegative.csv",mode="append", header=True)
    maxNeutral.write.csv("/user/soa/cetrangolo_santonastaso/risultati/maxLikeSentimentNeturral.csv",mode="append", header=True)

def maxHashtagSentiment(*dataframe):
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

            maxPositive.write.csv("/user/soa/cetrangolo_santonastaso/risultati/Positive_max_Hashtag_"+i+".csv",mode="append", header=True)
            maxNegative.write.csv("/user/soa/cetrangolo_santonastaso/risultati/Negative_max_Hashtag_"+i+".csv",mode="append", header=True)
            maxNeutral.write.csv("/user/soa/cetrangolo_santonastaso/risultati/Neutral_max_Hashtag_"+i+".csv",mode="append", header=True)
        except :
            #print("Mese saltato: "+i)
            continue





def main():

    pathDetails= "/user/soa/cetrangolo_santonastaso/Summary_month/Summary_Details/Summary_Details_year.csv"
    #"/Users/cetra/Desktop/dataset/SummarDetails.csv" #sys.argv[1]
    pathHashtag ="/user/soa/cetrangolo_santonastaso/Summary_month/Summary_Hashtag/Summary_Hashtag_bigfile.csv"
    #"/Users/cetra/Desktop/dataset/Summary_Hashtag.csv"  # sys.argv[1]
    pathSentiment ="/user/soa/cetrangolo_santonastaso/Summary_month/Summary_Sentiment/Summary_Sentiment_year.csv"
    #"/Users/cetra/Desktop/dataset/Summary_Sentiment.csv"  # sys.argv[1]

    dfDetails = spark.read.option("inferSchema", "true").option("header", "true").csv(pathDetails)
    dfHashtag = spark.read.option("inferSchema", "true").option("header", "true").csv(pathHashtag)
    dfSentiment = spark.read.option("inferSchema", "true").option("header", "true").csv(pathSentiment)

    topLang(dfDetails)
    topLangMonth(dfDetails)
    topHashtag(dfHashtag)
    topHashtagMonth(dfDetails,dfHashtag)
    topSentiment(dfSentiment)
    topSentimentMonth(dfDetails, dfSentiment)
    avgTweetsMonth(dfDetails)
    totTweets_Retweets_Month(dfDetails)
    languageSentiment(dfDetails,dfSentiment)
    summaryMonth(dfDetails)
    maxLikeSentiment(dfDetails,dfSentiment)
    maxHashtagSentiment(dfHashtag,dfSentiment,dfDetails)
    topAllDay(dfDetails,dfSentiment,dfHashtag)


start_time_tot=time.time()
start_time_proc=time.clock()

main()

finish_time_proc=time.clock()-start_time_proc+" seconds time_processore"
finish_time_tot=time.time()-start_time_tot+" seconds time_totale"
file=open("/user/soa/cetrangolo_santonastaso/file_timestamp.txt","w")
file.write(finish_time_tot)
file.write(finish_time_proc)
file.close()

spark.stop()
print("Fine")

