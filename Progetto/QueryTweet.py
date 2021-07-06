import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, instr

spark = SparkSession.builder.appName("Query_su_Tweet_Covid19").getOrCreate()

listmese="Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"


def topLang(dataframe):
    super = dataframe.groupby("Language").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topLang.csv", header=False)

def topLangMonth(dataframe):
    for i in listmese:
        super = dataframe.filter(instr(dataframe["Date Created"], i)>=1).groupby("Language").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/Users/cetra/Desktop/risultati/topLangMonth_"+i+".csv", header=False)

def topHashtag(dataframe):
    super = dataframe.groupby("Hashtag").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topHashtag.csv", header=False)

def topHashtagMonth(*dataframe):
    dfDetails, dfHashtag = dataframe
    dfjoin = dfHashtag.join(dfDetails, "Tweet_ID", 'inner')
    for i in listmese:
        super = dfjoin.filter(instr(dfjoin["Date Created"], i)>=1).groupby("Hashtag").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/Users/cetra/Desktop/risultati/topHashtagMonth_"+i+".csv", header=False)


def topSentiment(dataframe):
    super = dataframe.groupby("Sentiment_Label").count().orderBy("count", ascending=False)
    super.write.csv("/Users/cetra/Desktop/risultati/topSentiment.csv", header=False)

def topSentimentMonth(*dataframe:pyspark.sql.dataframe.DataFrame):
    dfDetails, dfSentiment = dataframe
    dfjoin = dfSentiment.join(dfDetails, "Tweet_ID", 'inner')
    for i in listmese:
        super = dfjoin.filter(instr(dfjoin["Date Created"], i)>=1).groupby("Sentiment_Label").count().withColumnRenamed("count",i).orderBy(i,ascending=False)
        super.write.csv("/Users/cetra/Desktop/risultati/topSentimentMonth_"+i+".csv", header=False)

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

def joinSentimentDetails(*dataframe):
    dfDetails, dfSentiment=dataframe
    super = dfSentiment.join(dfDetails, "Tweet_ID", 'inner').select("Date Created", "Sentiment_Label","Tweet_ID").limit(5)
    super.write.csv("/Users/cetra/Desktop/risultati/joinSentDet.csv", header=True)




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
    #languageSentiment(dfDetails,dfSentiment)
    #summaryMonth(dfDetails)
    #joinSentimentDetails(dfDetails,dfSentiment)








main()
spark.stop()
print("Fine")

