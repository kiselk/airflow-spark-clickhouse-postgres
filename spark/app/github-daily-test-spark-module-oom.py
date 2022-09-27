from pyspark import SparkContext, SparkFiles
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, split, udf
#import gender_guesser.detector as gender
from pyspark.sql.functions import udf
import re
import requests
from datetime import datetime
import os
from urllib.request import urlopen, Request
from gzip import decompress
OUTPUT_DIR = r'/usr/local/airflow/airflow/output'
TMP_DIR = os.path.join(OUTPUT_DIR, "tmp")
NOW = datetime.now()
CURRENT_DATE_STR = NOW.strftime('%Y-%m-%d')
CURRENT_HOUR_STR = NOW.hour
# - Spark-Submit cmd: spark-submit --master spark://spark:7077
# --conf spark.master=spark://spark:7077 --name github-daily-test-spark-module
# --verbose --queue root.default /usr/local/spark/app/github-daily-test-spark-module.py
# [2022-09-26, 17:59:03 UTC] {spark_submit.py:485} INFO - Using properties file: null
spark = (SparkSession
         .builder
         .config("spark.driver.extraJavaOptions", "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'")
         .config("spark.executor.extraJavaOptions", "-Dhttp.agent='Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0'")
         .getOrCreate()
         )
sc = spark.sparkContext
#gdetector = gender.Detector()


def write_to_csv(df, df_name):
    df.write.format("csv").save(
        OUTPUT_DIR + '/' + CURRENT_DATE_STR + '/' + df_name)


def list_urls():
    urls = []
    paths = []
    names = []
    TMP_DIR = os.path.join(OUTPUT_DIR, "tmp")
    NOW = datetime.now()
    CURRENT_DATE_STR = NOW.strftime('%Y-%m-%d')
    CURRENT_HOUR_STR = NOW.hour

    for hour in range(2):  # int(CURRENT_HOUR_STR)):
        try:
            prefix = CURRENT_DATE_STR + '-' + str(hour)
            url = 'http://data.gharchive.org/' + prefix + '.json.gz'
            name = prefix + '.json.gz'
            path = os.path.join(OUTPUT_DIR, )
            urls.append(url)
            names.append(name)
            paths.append(path)
        except Exception as e:

            print("Could not download " + url + "\n" +
                  "2 Assuming no data for current hour" + "\n"
                  + str(e))
            break
    return urls, paths, names


urls, paths, names = list_urls()
# print("reading urls: " + ','.join(urls))

# print("transforming names: " + ','.join(names))
# files = ['file:///usr/local/airflow/airflow/output/' + name for name in names]
# print(" reading files" + ','.join(files))

print("==================================================")
# print("urls: " + ','.join(urls))
# for url in urls:
#     spark.sparkContext.addFile(url)
# parsed = [r'file://' + SparkFiles.get(file) for file in names]
# print("parsed: " + ','.join(parsed))
req = Request(
    urls[0],
    data=None,
    headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    }
)
jsonData = decompress(urlopen(req).read()).decode('utf-8')

json = sc.parallelize(jsonData).toDF("json")
print(json.show())
#json = spark.read.json(tmp)
print("==================================================")


json.createOrReplaceTempView("github")

eventsDF = spark.sql("SELECT type,repo.name as owner_repo,actor.login as submitter FROM github") \
    .withColumn("owner", split(col("owner_repo"), "/").getItem(0)) \
    .withColumn("repo", split(col("owner_repo"), "/").getItem(1)) \
    .drop("owner_repo")

commitsDF = eventsDF.where("type='PushEvent'")
submitsDF = commitsDF.groupBy('submitter').count()

# List of Developers that own more than one repository;
devsWithMoreThanOneRepoDF = eventsDF.drop('submitter', 'type').distinct(
).groupBy('owner').count().where("count > 1").orderBy(col('count').desc())


# List of Developers who did more than one commit in a day, ordered by name and number of commits;
moreThanOneCommitSubmittersDF = submitsDF.where(
    "count > 1").orderBy(col('count').desc())
# List of Developers with less than one commit in a day;
lessThanOneCommitSubmittersDF = submitsDF.where(
    "count <= 1").orderBy(col('count').desc())

# Total Developers grouped by gender;
# API does not provide gender
namesDF = eventsDF.select('owner').withColumnRenamed('owner', 'name').union(
    eventsDF.select('submitter').withColumnRenamed('submitter', 'name')).distinct()
gendersDF = namesDF  # .withColumn(
# "gender", detectGender()("name")).groupBy("gender").count()


# Total projects with more than 10 members;
# eventsDF.groupBy('owner').count()
reposWithMoreThanTenMembersDF = eventsDF.drop('type').distinct().groupBy(
    "owner", "repo").count().where("count > 10").orderBy(col('count').desc())


print("List of Developers that own more than one repository")
devsWithMoreThanOneRepoDF.show()
write_to_csv(devsWithMoreThanOneRepoDF, 'devsWithMoreThanOneRepoDF')


print("List of Developers who did more than one commit in a day, ordered by name and number of commits")
moreThanOneCommitSubmittersDF.show()
write_to_csv(moreThanOneCommitSubmittersDF, 'moreThanOneCommitSubmittersDF')

print("List of Developers with less than one commit in a day")
lessThanOneCommitSubmittersDF.show()
write_to_csv(lessThanOneCommitSubmittersDF, 'lessThanOneCommitSubmittersDF')

print("Total Developers grouped by gender")
gendersDF.show()
write_to_csv(gendersDF, 'gendersDF')

print("Total projects with more than 10 members")
reposWithMoreThanTenMembersDF.show()
write_to_csv(reposWithMoreThanTenMembersDF, 'reposWithMoreThanTenMembersDF')
