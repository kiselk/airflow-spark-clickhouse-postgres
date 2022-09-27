from pyspark import SparkContext
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, split, udf
#import gender_guesser.detector as gender
from pyspark.sql.functions import udf
import re
import requests
from datetime import datetime
import os
OUTPUT_DIR = r'/usr/local/spark/output'
TMP_DIR = os.path.join(OUTPUT_DIR, "tmp")
NOW = datetime.now()
CURRENT_DATE_STR = NOW.strftime('%Y-%m-%d')
CURRENT_HOUR_STR = NOW.hour
os.mkdir(OUTPUT_DIR)
os.mkdir(TMP_DIR)
spark = (SparkSession
         .builder
         .config("spark.sql.warehouse.dir", OUTPUT_DIR)
         .getOrCreate()
         )
sc = spark.sparkContext
#gdetector = gender.Detector()


def write_to_csv(df, df_name):
    df.write.format("csv").save(
        OUTPUT_DIR + '/' + CURRENT_DATE_STR + '/' + df_name)


def download(date):
    paths = []
    for hour in range(int(CURRENT_HOUR_STR)):
        try:
            prefix = date + '-' + str(hour)
            url = 'https://data.gharchive.org/' + prefix + '.json.gz'
            path = os.path.join(OUTPUT_DIR, prefix + '.json.gz')
            response = requests.get(url, stream=True)
            print("Trying to save into: " + str(path))
            open(path, 'wb+').write(response.content)
            paths.append(path)
        except Exception as e:

            print("Could not download " + url + "\n" +
                  "2 Assuming no data for current hour" + "\n"
                  + str(e))
            break
    return paths


def detectGender():
    def detectGender_(col):
        #clean = re.sub('[^a-zA-Z]+', '', col)
        names = re.sub(r"([A-Z])", r" \1", col).split()
        clean = re.sub('[^a-zA-Z]+', '', names[0])
        #names = re.split('(?=[A-Z])', clean)
        # print(clean)
        # return gdetector.get_gender(clean)
    return udf(detectGender_, StringType())


files = download(CURRENT_DATE_STR)
if len(files) == 0:
    raise(Exception("no files to process"))


json = spark.read.json(files)
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
