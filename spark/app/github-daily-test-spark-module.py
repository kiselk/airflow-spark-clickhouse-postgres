from pyspark import SparkContext, SparkFiles
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, split, udf
#import gender_guesser.detector as gender
from pyspark.sql.functions import udf
import os

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
    df.coalesce(1).write.format("csv").mode("overwrite")\
        .save("/usr/local/spark/resources/data/" + df_name, header=True)


print("==================================================")
file = '2011-02-12-12.json.gz'
gh = 'http://data.gharchive.org/'
sc.addFile(gh + file)
path = SparkFiles.get(file)
print("SparkFiles root: " + SparkFiles.getRootDirectory())
print("SparkFiles root contents" +
      ','.join(os.listdir(SparkFiles.getRootDirectory())))
json = spark.read.json(path)
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
