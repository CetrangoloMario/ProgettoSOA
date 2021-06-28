import csv
import glob
import sys

#pathCartellaUnire = input("Enter path cartella unire: ")
#pathOutput= input("Output path con finale: ")
#name_output=input(" Nome file output .csv: ")

#spark-submit --master local --executor-memory 5G --num-executors 4 --executor-cores 3
 #\Users\manlio\Desktop\ScriptConcatenaCSV.py
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/Summary_Details/2020_01"
 #/Users/manlio/Desktop/COVID19_Tweets_Dataset/AllSummaryDetails1.csv

pathCartellaUnire = sys.argv[1]
pathOutput =sys.argv[2]

print(pathCartellaUnire)
print(pathOutput)

all_filenames=[i for i in glob.glob(pathCartellaUnire+"/*.csv")]

dest_file=pathOutput

content=[]
for file_name in sorted(all_filenames):
    with open(file_name, "r") as f:
        lines=f.readlines()

    for i,l in enumerate(lines):
        if len(content)==0:
            content.append(l)

        elif i !=0:
            content.append(l)

with open(dest_file, "w") as f:
    f.writelines(content)


print("OK")