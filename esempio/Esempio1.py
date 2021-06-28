from pyspark.sql import SparkSession


def main1():
    spark = SparkSession.builder.appName("esempio").getOrCreate()
    person = spark.createDataFrame([
        (0, "Bill Chambers", 0, [100]),
        (1, "Matei Zaharia", 1, [500, 250, 100]),
        (2, "Michael Armbrust", 1, [250, 100])]) \
        .toDF("id", "name", "graduate_program", "spark_status")
    graduateProgram = spark.createDataFrame([
        (0, "Masters", "School of Information", "UC Berkeley"),
        (2, "Masters", "EECS", "UC Berkeley"),
        (1, "Ph.D.", "EECS", "UC Berkeley")]) \
        .toDF("id", "degree", "department", "school")
    sparkStatus = spark.createDataFrame([
        (500, "Vice President"),
        (250, "PMC Member"),
        (100, "Contributor")]) \
        .toDF("id", "status")

    person.show()
    graduateProgram.show()
    sparkStatus.show()





if __name__ == '__main__':
    main1()
